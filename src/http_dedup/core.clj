(ns http-dedup.core
  (:gen-class)
  (:import [jline.console ConsoleReader])
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.tools.cli :as cli]
            [taoensso.timbre :as log]
            [http-dedup
             [buffer-manager :as bufman]
             [socket-manager :as sockman]
             [air-traffic-controller :as atc]
             [select :as select]
             [async-utils :refer [go-loop-<!]]
             [util :refer [bytebuf-to-str str-to-bytebuf]]]))

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
       (let [new-read-channel (async/chan)]
         (async/onto-chan new-read-channel bufs false)
         (async/pipe read-channel new-read-channel)
         (atc/board atc request write-channel new-read-channel))))))

(defn run-server [listen-addr listen-port connect-addr connect-port]
  (let [controlch (async/chan)
        bufman (bufman/buffer-manager 16 32768)
        select (select/select)
        sockman (sockman/socket-manager select bufman)
        connch (sockman/listen sockman listen-addr listen-port)
        atc (atc/air-traffic-controller sockman connect-addr connect-port)]
    (go-loop-<!
     connch socket
     (apply handle-incoming atc (<! (sockman/accept sockman socket))))
    (go-loop []
             (if-let [[msg & args] (<! controlch)]
               (do (case msg
                     :bufman (>! bufman [:debug-state])
                     :sockman (>! sockman [:debug-state])
                     :atc (>! atc [:debug-state])
                     :select (>! select [:debug-select-thread])
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
    :assoc-fn (fn [m k _] (update-in m [k] inc))]])

(def ^java.text.SimpleDateFormat ^:private iso8601 (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
(.setTimeZone iso8601 (java.util.TimeZone/getTimeZone "UTC"))

(defn iso8601-format
  "Format a date using ISO8601 format. Always UTC."
  [date]
  (.format iso8601 date))

(def ns-colors (ref {}))
(def next-ns-color (ref 1))
(defn ns-color [ns]
  (when-not (@ns-colors ns)
    (dosync (alter ns-colors assoc ns (alter next-ns-color #(mod (inc %) 8)))))
  (@ns-colors ns))
(defn- color-str [i]
  (format "\u001b[%dm" i))

(defn pretty-log
  [cr {:keys [level throwable instant ns message]}]
  (let [base-color (case level
                     (:warn :error) 31
                     (:info) 36
                     0)]
    (locking cr ; so we don't trip on ourselves
      (println (format "\r%s%s %s [%s%s%s] - %s%s"
                       (color-str base-color)
                       (iso8601-format instant)
                       (-> level name clojure.string/upper-case)
                       (color-str (+ 30 (ns-color ns))) ns (color-str base-color)
                       (or message "") (or (log/stacktrace throwable "\n") "")))))
  (.drawLine cr)
  (.flush cr))

(defn -main
  [& args]
  (let [{{:keys [verbosity listen connect]} :options
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

          (log/info "Listening on" listen " -- fowarding connections to" connect)
          (let [server (apply run-server (into listen connect))
                reader (ConsoleReader.)]
            (log/set-config! [:appenders :pretty] {:enabled? true
                                                   :fn (partial pretty-log reader)})
            (log/set-config! [:appenders :standard-out :enabled?] false)
            (loop []
              (when-let [line (.readLine reader "http-dedup> ")]
                (let [[cmd & args] (clojure.string/split line #"\s+")]
                  (case (keyword cmd)
                    :quit nil
                    :bufman (async/>!! server [:bufman])
                    :sockman (async/>!! server [:sockman])
                    :atc (async/>!! server [:atc])
                    :select (async/>!! server [:select])
                    (println "Unrecognized command: " cmd))
                  (when-not (= cmd "quit") (recur)))))
            (async/close! server))))))
