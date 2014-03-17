(ns http-dedup.core
  (:gen-class)
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.tools.cli :as cli]
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

(defn run-server [listen-addr listen-port connect-addr connect-port]
  (let [controlch (async/chan)
        sockman (sockman/socket-manager)
        connch (sockman/listen sockman listen-addr listen-port)
        atc (atc/air-traffic-controller sockman connect-addr connect-port)]
    (go-loop-<!
     connch socket
     (apply handle-incoming atc (<! (sockman/accept sockman socket))))
    (go (<! controlch)
        (async/close! sockman)
        (async/close! atc))
    controlch))

(def cli-options
  [["-l" "--listen [HOST:]PORT" "Listen address"
    :default [nil 8081]
    :parse-fn #(let [[_ h p] (re-matches #"(?:([^:]+):)?(\d+)" %)] [h (Integer/parseInt p)])
    :validate [#(< 0 (second %) 0x10000) "Port must be 1-65535"]]
   ["-c" "--connect [HOST:]PORT" "Connect address"
    :default [nil 8080]
    :parse-fn #(let [[_ h p] (re-matches #"(?:([^:]+):)?(\d+)" %)] [h (Integer/parseInt p)])
    :validate [#(< 0 (second %) 0x10000) "Port must be 1-65535"]]
   ["-v" "--verbose" "Verbosity level"
    :id :verbosity
    :default 0
    :assoc-fn (fn [m k _] (update-in m [k] inc))]])

(defn -main
  [& args]
  (let [{{:keys [verbosity listen connect]} :options
         summary :summary
         errs :errors} (cli/parse-opts args cli-options)]
    (if errs
      (do (println errs)
          (println "Usage: http-dedup [opts]")
          (println summary))
      (do (taoensso.timbre/set-level! (condp < verbosity
                                        1 :trace
                                        0 :debug
                                        :info))
          (async/<!! (apply run-server (into listen connect)))))))
