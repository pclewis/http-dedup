(ns http-dedup.core
  (:gen-class)
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.tools.cli :as cli]
            [taoensso.timbre :as log]
            [http-dedup interactive pretty-log
             [buffer-manager :as bufman]
             [socket-manager :as sockman]
             [air-traffic-controller :as atc]
             [select :as select]
             [util :refer [bytebuf-to-str str-to-bytebuf]]]))

(defmethod clojure.core/print-method Throwable [t ^java.io.Writer writer]
  (.write writer (log/stacktrace t "\n" {})))

(defn drop-bytes!
  "Modify a sequence of buffers so that the first n bytes are removed.
   If n is bigger than the first buffer, it will have .remaining=0, and so on."
  [n bufs]
  (when-let [[buf & rest] (seq bufs)]
    (let [size (.remaining buf)]
      (if (> n size)
        (do (.position buf (.limit buf))
            (recur (- n size) rest))
        (.position buf (+ (.position buf) n))))))

(defn prepare-request
  "Change Connection: keep-alive to Connection: close and manage buffers
   appropriately. Necessary because the buffers may have already started
   reading the body of the request, which we need to preserve."
  [req bufs]
  (drop-bytes! (count req) bufs)
  (let [new-req (clojure.string/replace-first req
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
       (let [new-read-channel (async/chan)]
         (async/onto-chan new-read-channel bufs false)
         (async/pipe read-channel new-read-channel)
         (atc/board atc request write-channel new-read-channel))))))

(defn run-server [listen-addr listen-port connect-addr connect-port]
  (let [controlch (async/chan)
        bufman (bufman/buffer-manager 2048 32768)
        select (select/select)
        sockman (sockman/socket-manager select bufman)
        connch (sockman/listen sockman listen-addr listen-port)
        atc (atc/air-traffic-controller sockman connect-addr connect-port)]

    (go-loop []
      (when-let [[rch wch] (<! connch)]
        (handle-incoming atc rch wch)
        (recur)))

    (go-loop []
             (if-let [[msg & args] (<! controlch)]
               (do (case msg
                     :bufman (>! bufman [:debug-state])
                     :sockman (>! sockman [:debug-state])
                     :atc (>! atc [:debug-state])
                     :select (select/debug-select-thread select)
                     nil)
                   (recur))
               (do (async/close! sockman)
                   (async/close! atc))))
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
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   ["-d" "--daemon" "Run as daemon - default if no terminal attached"
    :default (nil? (System/console))]])

(defn -main
  [& args]
  (let [{{:keys [verbosity listen connect daemon]} :options
         summary :summary
         errs :errors} (cli/parse-opts args cli-options)]
    (if errs
      (do (println errs)
          (println "Usage: http-dedup [opts]")
          (println summary))
      (do (log/set-level! (condp < verbosity
                            1 :trace
                            0 :debug
                            :info))
          (if daemon
            (log/set-config! [:appenders :standard-out :fmt-output-opts :nofonts?] true)
            (do (log/set-config! [:appenders :pretty] {:enabled? true :fn http-dedup.pretty-log/pretty-log})
                (log/set-config! [:appenders :standard-out :enabled?] false)))
          (let [server (apply run-server (into listen connect))]
            (log/info "Listening on" listen " -- forwarding connections to" connect)
            (if daemon
              (do (.close System/in)
                  (async/<!! server))
              (do (http-dedup.interactive/session server)
                  (async/close! server))))))))
