(ns http-dedup.air-traffic-controller
  (:require [clojure.core.async :as async :refer [go go-loop >! <!]]
            [http-dedup.async-utils :refer :all]
            [http-dedup.socket-manager :as sockman]
            [taoensso.timbre :as log]))

; how to take a metaphor too far

(defasync air-traffic-controller [flights sockman host port this]
  (create [sockman host port]
          {:sockman sockman
           :host host
           :port port
           :flights {}})

  (destroy (async/close! sockman))

  (fn accept-passengers [jetway]
    (async/reduce (fn [ps [p bs]]
                    (dorun (map #(sockman/return-buffer sockman %) bs))
                    (conj ps p))
                  [] jetway))

  (fn start-flight [destination]
    (let [jetway (async/chan) ; receives [channel [buffers]]
          dest-name (first (clojure.string/split-lines destination))]
      (go
       (log/trace "start-flight: boarding to" dest-name)
       (let [[pilot flight-plan] (<! jetway)
             boarding (accept-passengers jetway)
             [read-channel write-channel] (->> (sockman/connect sockman host port) <!
                                               (sockman/accept sockman) <!)
             _ (async/onto-chan write-channel flight-plan false)
             first-block (<! read-channel) ; don't take off till .. the analogy breaks down
             _ (depart this destination)   ; stop sending new passengers
             passengers (<! boarding)]
         (log/info "start-flight: departing to" dest-name "with" (inc (count passengers)) "passengers")
         (loop [buf first-block]
           (doseq [p passengers] (>! p (<! (sockman/copy-buffer sockman buf))))
           (when-not (>! pilot buf) (sockman/return-buffer sockman buf))
           (recur (<! read-channel)))
         (doseq [p (conj passengers pilot)] (async/close! p))))
      jetway))

  (board
   [destination passenger bags]
   (let [flight (or (get flights destination)
                    (start-flight destination))]
     (>! flight [passenger bags]) ; no race condition: only we close flight channel
     {:flights (assoc flights destination flight)}))

  (depart
   [destination]
   (async/close! (get flights destination))
   {:flights (dissoc flights destination)}))
