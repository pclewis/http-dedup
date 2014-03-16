(ns http-dedup.air-traffic-controller
  (:require [clojure.core.async :as async :refer [go go-loop >! <!]]
            [http-dedup.async-utils :refer :all]
            [http-dedup.socket-manager :as sockman]))

; how to take an analogy too far

(defasync air-traffic-controller [flights sockman host port this]
  ([sockman host port] {:sockman sockman :host host :port port :flights {}})

  (fn dump-bags [ps [p bs]]
    (doseq [b bs]
      (sockman/return-buffer sockman b))
    (conj ps p))

  (fn start-flight [destination]
    (let [jetway (async/chan)]
      (go
       (let [[first-passenger req-bags] (<! jetway)
             boarding (async/reduce dump-bags [first-passenger] jetway) ; start accepting passengers
             new-sock (<! (sockman/connect sockman host port))
             [read-channel write-channel] (<! (sockman/accept new-sock))
             _ (async/onto-chan write-channel req-bags false)
             first-block (<! read-channel)          ; wait for first block
             _ (>! this [:depart destination])      ; let atc know we're leaving so they close the jetway
             passengers (<! boarding)]              ; find out who boarded
         (loop [block first-block]
           (when block
             ; look at this. LOOK AT THIS.
             (doall (map >! passengers (iterate #(sockman/copy-buffer sockman %) block)))
             (recur (<! write-channel))))
         (doseq [p passengers]
           (async/close! p)))
       jetway)))

  (board
   [destination passenger bags]
   (let [flight (or (get flights destination)
                    (start-flight destination))]
     (>! flight [passenger bags])
     {:flights (assoc flights destination flight)}))

  (depart
   [destination]
   {:flights (dissoc flights destination)}))
