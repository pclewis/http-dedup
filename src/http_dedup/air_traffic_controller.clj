(ns http-dedup.air-traffic-controller
  (:require [clojure.core.async :as async :refer [go go-loop >! <!]]
            [http-dedup.async-utils :refer :all]
            [http-dedup.socket-manager :as sockman]
            [http-dedup.actor :refer [defactor run-actor]]
            [taoensso.timbre :as log]))

; how to take a metaphor too far

(defactor AirTrafficController
  (board [destination passenger bags])
  (depart [destination]))

(defn accept-passengers [jetway trash-chute]
  (async/reduce (fn [ps [p bs]]
                  (async/pipe bs trash-chute false)
                  (conj ps p))
                [] jetway))

(defn start-flight [{:keys [sockman host port trash-chute]} atc destination]
  (let [jetway (async/chan)      ; receives [channel [buffers]]
        dest-name (first (clojure.string/split-lines destination))]
    (go
     (try
       (log/trace "start-flight: boarding to" dest-name)
       (let [[pilot flight-plan] (<! jetway)
             boarding (accept-passengers jetway trash-chute)]
         (if-let [[read-channel write-channel] (<! (sockman/connect-and-accept sockman host port))]
           (do
             (async/pipe flight-plan write-channel false)
             (let [first-block (<! read-channel)
                   _ (depart atc destination) ; stop accepting new passengers after first block read
                   passengers (<! boarding)]
               (log/info "start-flight: departing to" dest-name "with" (inc (count passengers)) "passengers")
               (loop [buf first-block]
                 (when buf
                   (doseq [p passengers :let [copy (<! (sockman/copy-buffer sockman buf))]]
                     (or (>! p copy) (>! trash-chute copy)))
                   (or (>! pilot buf) (>! trash-chute buf))
                   (recur (<! read-channel))))
               (doseq [p (conj passengers pilot)] (async/close! p))
               (async/close! write-channel)
               (log/debug "start-flight: flight to" dest-name "finished")))
           (do (log/warn "start-flight: connection failed, canceling flight to" dest-name)
               (async/pipe flight-plan trash-chute false)
               (depart atc destination)
               (doseq [p (<! boarding)]
                 (async/close! p))
               (async/close! pilot))))
       (catch Throwable t
         (log/error t "start-flight: aborting flight due to exception"))))
    jetway))

(defrecord ATC [flights sockman host port trash-chute actor]
  AirTrafficController
  (board [this out destination passenger bags]
    (let [flight (or (get flights destination)
                     (start-flight this actor destination))]
      (async/put! flight [passenger bags]) ; no race condition: only we close flight channel
      {:flights (assoc flights destination flight)}))

  (depart [this out destination]
    (async/close! (get flights destination))
    {:flights (dissoc flights destination)} ))


(defn air-traffic-controller [sockman host port]
  (let [actor (AirTrafficControllerActor. (async/chan))
        trash-chute (async/map> #(vector :return-buffer nil %) sockman)]
    (go
     (<! (run-actor actor (ATC. {} sockman host port trash-chute actor)))
     (async/close! trash-chute) ; note: also closes sockman
     (async/close! sockman))
    actor))
