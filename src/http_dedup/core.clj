(ns http-dedup.core
  (:gen-class)
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [http-dedup.socket-manager :as sockman]
            [http-dedup.air-traffic-controller :as atc]
            [http-dedup.async-utils :refer [go-loop-<!]]
            [http-dedup.util :refer [bytebuf-to-str str-to-bytebuf]]))

(defn drop-bytes
  [n bufs]
  (reduce (fn [x buf]
            (let [size (- (.limit buf) (.position buf))
                  btd (min x size)]
              (.position buf (+ (.position buf) btd))
              (- x btd)))
          n bufs)
  bufs)


(defn prepare-request
  "Change Connection: keep-alive to Connection: close and manage buffers
   appropriately. Necessary because the buffers may have already started
   reading the body of the request, which we need to preserve."
  [req bufs]
  (let [new-bufs (drop-bytes (count req) bufs)
        new-req (clojure.string/replace-first req
                                              #"(?m)^Connection: (.*)$"
                                              "Connection: close")]
    [new-req (into [(str-to-bytebuf new-req)] bufs)]))

(defn read-request [read-channel]
  (go-loop [s nil
            bufs []]
           (when-let [buf (<! read-channel)]
             (let [ns (str s (bytebuf-to-str buf))
                   i (.indexOf ns "\r\n\r\n")]
               (if (>= i 0)
                 (prepare-request (subs ns 0 i) (conj bufs buf))
                 (recur ns (conj bufs buf)))))))

(defn handle-incoming [atc read-channel write-channel]
  (go
   (let [[request bufs] (<! (read-request read-channel))]
     (when request
       (atc/board atc request write-channel bufs)))))

(defn run-server [listen-port connect-port]
  (let [controlch (async/chan)
        sockman (sockman/socket-manager)
        connch (sockman/listen sockman "0.0.0.0" listen-port) ; FIXME: localhost
        atc (atc/air-traffic-controller sockman nil connect-port)]
    (go-loop-<!
     connch socket
     (apply handle-incoming atc (<! (sockman/accept sockman socket))))
    (go (<! controlch)
        (async/close! sockman)
        (async/close! atc))
    controlch))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (taoensso.timbre/set-level! :trace)
  (taoensso.timbre/merge-config! {:timestamp-pattern "yyyy-MM-dd HH:mm:ss"} )

  (let [[listen-port connect-port] (map #(Integer. %) args)]
    (if (and (< 0 listen-port 65536)
             (< 0 connect-port 65536))
      (async/<!! (run-server listen-port connect-port))
      (println "Usage: http-dedup [listen-port] [connect-port]"))))
