(ns http-dedup.air-traffic-controller
  (:require [clojure.core.async :as async :refer [go go-loop >! <!]]
            [http-dedup.async-utils :refer :all]
            [http-dedup.socket-manager :as sockman]
            [taoensso.timbre :as log]))

; how to take a metaphor too far

(defasync air-traffic-controller [flights sockman host port this]
  (create [sockman host port] {:sockman sockman :host host :port port :flights {}})
  (destroy (async/close! sockman))

  (fn dump-bags [ps [p bs]]
    (doseq [b bs]
      (sockman/return-buffer sockman b))
    (conj ps p))

  (fn start-flight [destination]
    (let [jetway (async/chan)
          dest-name (first (clojure.string/split-lines destination))]
      (go
       (log/trace "Starting flight to" dest-name)
       (let [[first-passenger req-bags] (<! jetway)
             boarding (async/reduce dump-bags [] jetway) ; start accepting passengers
             new-sock (<! (sockman/connect sockman host port))
             [read-channel write-channel] (<! (sockman/accept sockman new-sock))
             _ (async/onto-chan write-channel req-bags false)
             first-block (<! read-channel) ; wait for first block
             _ (depart this destination)   ; let atc know we're leaving so they close the jetway
             other-passengers (<! boarding)] ; find out who boarded
         (log/info "Flight to" dest-name "has" (inc (count other-passengers)) "passengers")
         (loop [block first-block]
           (when block

             (doseq [p other-passengers]
               (let [b (<! (sockman/copy-buffer sockman block))]
                 (when-not (>! p b)
                   (sockman/return-buffer sockman b))))
             (when-not (>! first-passenger block)
               (sockman/return-buffer sockman block))
             (recur (<! read-channel))))
         (doseq [p (conj other-passengers first-passenger)]
           (async/close! p))))
      jetway))

  (board
   [destination passenger bags]
   (let [flight (or (get flights destination)
                    (start-flight destination))]
     (>! flight [passenger bags])
     {:flights (assoc flights destination flight)}))

  (depart
   [destination]
   (async/close! (get flights destination))
   {:flights (dissoc flights destination)}))
