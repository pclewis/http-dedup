(ns http-dedup.air-traffic-controller
  (:require [clojure.core.async :as async :refer [go go-loop >! <! alt!]]
            [http-dedup.async-utils :refer :all]
            [http-dedup.socket-manager :as sockman]
            [http-dedup.buffer-manager :as bufman]
            [http-dedup.select :as select]
            [http-dedup.actor :refer [defactor run-actor]]
            [http-dedup.util :refer [bytebuf-to-str str-to-bytebuf drop-bytes!]]
            [taoensso.timbre :as log]))

; how to take a metaphor too far

(defactor AirTrafficController
  (board [destination passenger bags])
  (depart [destination]))

(defn- accept-passengers [jetway trash-chute]
  (async/reduce (fn [ps [p bs]]
                  (async/pipe bs trash-chute false)
                  (conj ps p))
                [] jetway))

(defn- start-flight [{:keys [sockman config trash-chute actor]} destination]
  (let [jetway (async/chan)      ; receives [channel [buffers]]
        dest-name (first (clojure.string/split-lines destination))
        start (System/currentTimeMillis)]
    (go
     (try
       (log/trace "start-flight: boarding to" dest-name)
       (let [[pilot flight-plan] (<! jetway)
             boarding (accept-passengers jetway trash-chute)]
         (if-let [[read-channel write-channel] (<! (apply sockman/connect sockman (:connect config)))]
           (do
             (async/pipe flight-plan write-channel false)
             (let [first-block (<! read-channel)
                   first-block-time (System/currentTimeMillis)
                   _ (depart actor destination) ; stop accepting new passengers after first block read
                   passengers (<! boarding)]
               (log/debug "start-flight: departing to" dest-name "with" (inc (count passengers)) "passengers")
               (loop [buf first-block]
                 (when buf
                   (doseq [p passengers :let [copy (<! (sockman/copy-buffer sockman buf))]]
                     (or (>! p copy) (>! trash-chute copy)))
                   (or (>! pilot buf) (>! trash-chute buf))
                   (recur (<! read-channel))))
               (doseq [p (conj passengers pilot)] (async/close! p))
               (async/close! write-channel)
               (let [end (System/currentTimeMillis)]
                 (log/infof "\"%s\" - %d passengers - %dms total (%dms waiting, %dms sending)"
                            dest-name
                            (inc (count passengers))
                            (- end start)
                            (- first-block-time start)
                            (- end first-block-time)))
               (log/debug "start-flight: flight to" dest-name "finished")))
           (do (log/warn "start-flight: connection failed, canceling flight to" dest-name)
               (async/pipe flight-plan trash-chute false)
               (depart actor destination)
               (doseq [p (<! boarding)]
                 (async/close! p))
               (async/close! pilot))))
       (catch Throwable t
         (log/error t "start-flight: aborting flight due to exception"))))
    jetway))

(defrecord ATC [config flights sockman trash-chute actor]
  AirTrafficController
  (board [this out destination passenger bags]
    (let [flight (or (get flights destination)
                     (start-flight this destination))]
      (async/put! flight [passenger bags]) ; no race condition: only we close flight channel
      {:flights (assoc flights destination flight)}))

  (depart [this out destination]
    (async/close! (get flights destination))
    {:flights (dissoc flights destination)} ))

(defn- prepare-request
  "Change Connection: keep-alive to Connection: close and manage buffers
   appropriately. Necessary because the buffers may have already started
   reading the body of the request, which we need to preserve."
  [req bufs]
  (drop-bytes! (count req) bufs)
  (let [new-req (clojure.string/replace-first req
                                              #"(?m)^Connection: (.*)$"
                                              "Connection: close")]
    [new-req (into [(str-to-bytebuf new-req)] bufs)]))

(defn- read-request
  "Read ByteBuffers from a channel until two newlines in a row occur.
   Returns a channel which will receive the request as string and a
   vector of ByteBuffers that were pulled from the read-channel.
   If timeout-ms passes without reading from the socket, or the
   read-channel is closed, the request will be nil."
  [read-channel timeout-ms]
  (go-loop [request-so-far nil
            bufs []]
    (if-let [buf (alt! read-channel ([v] v)
                       (async/timeout timeout-ms) :timeout)]
      (if-not (= :timeout buf)
        (let [bs (conj bufs buf)
              s (str request-so-far (bytebuf-to-str buf))
              i (.indexOf s "\r\n\r\n")]
          (if (>= i 0)
            [(subs s 0 i) bs]
            (recur s bs)))
        (do (log/warn "Timeout reading from socket")
            [nil bufs]) )
      (do (log/warn "Connection closed before request received")
          [nil bufs]))))

(defn- handle-incoming [atc [read-channel write-channel]]
  (go
    (when-let [[request bufs] (<! (read-request read-channel 60000))]
      (if request
        (let [[request bufs] (prepare-request request bufs)
              refilled-read-channel (async/chan)]
          (async/onto-chan refilled-read-channel bufs false)
          (async/pipe read-channel refilled-read-channel)
          (board atc request write-channel refilled-read-channel))
        (async/onto-chan (:trash-chute atc) bufs)))))

(defn air-traffic-controller [config]
  (let [actor (AirTrafficControllerActor. (async/chan))
        bufman (bufman/buffer-manager (:max-buffers config) (:buffer-size config))
        select (select/select)
        sockman (sockman/socket-manager select bufman)
        trash-chute (async/map> #(vector :return-buffer nil %) sockman)]
    (log/info "Listening on" (:listen config) "-- forwarding to" (:connect config))
    (drain (apply sockman/listen sockman (:listen config))
           handle-incoming actor)
    (go
     (<! (run-actor actor (ATC. config {} sockman trash-chute actor)))
     (async/close! trash-chute) ; note: also closes sockman
     (async/close! sockman))
    actor))
