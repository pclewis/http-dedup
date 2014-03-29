(ns http-dedup.core
  (:gen-class)
  (:require [clojure.core.async :as async]
            [clojure.tools.cli :as cli]
            [taoensso.timbre :as log]
            [http-dedup interactive pretty-log
             [air-traffic-controller :as atc]]))

(defmethod clojure.core/print-method Throwable [t ^java.io.Writer writer]
  (.write writer ^String (log/stacktrace t "\n" {})))

(def cli-options
  [["-l" "--listen [HOST:]PORT"
    "Listen address"
    :default [nil 8081]
    :parse-fn #(let [[_ h p] (re-matches #"(?:([^:]+):)?(\d+)" %)]
                 [h (Integer/parseInt p)])
    :validate [#(< 0 (second %) 0x10000) "Port must be 1-65535"]]

   ["-c" "--connect [HOST:]PORT"
    "Connect address"
    :default [nil 8080]
    :parse-fn #(let [[_ h p] (re-matches #"(?:([^:]+):)?(\d+)" %)]
                 [h (Integer/parseInt p)])
    :validate [#(< 0 (second %) 0x10000) "Port must be 1-65535"]]

   ["-v" "--verbose"
    "Verbosity level"
    :id :verbosity
    :default 0
    :assoc-fn (fn [m k _] (update-in m [k] inc))]

   ["-d" "--daemon"
    "Run as daemon - default if no terminal attached"
    :default (nil? (System/console))]

   [nil "--max-buffers N"
    "Maximum ByteBuffers to allocate"
    :default 2048
    :parse-fn #(Integer/parseInt %)]

   [nil "--buffer-size BYTES"
    "Size of each ByteBuffer in bytes"
    :default 32768
    :parse-fn #(Integer/parseInt %)]

   [nil "--request-timeout MS"
    "Max time to wait for client to send request in ms"
    :default 60000
    :parse-fn #(Integer/parseInt %)]])

(defn -main
  [& args]
  (let [{{:keys [verbosity daemon] :as config} :options
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
            (log/set-config! [:appenders :standard-out :fmt-output-opts
                              :nofonts?] true)
            (do (log/set-config! [:appenders :pretty]
                                 {:enabled? true
                                  :fn http-dedup.pretty-log/pretty-log})
                (log/set-config! [:appenders :standard-out :enabled?] false)))
          (let [control (atc/run-server config)]
            (if daemon
              (do (.close System/in)
                  (async/<!! control))
              (do (http-dedup.interactive/session control)
                  (async/close! control))))))))
