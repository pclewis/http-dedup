(ns http-dedup.pretty-log
  (:import java.text.SimpleDateFormat
           java.util.TimeZone
           jline.console.ConsoleReader)
  (:require [taoensso.timbre :as log]))

(def ^SimpleDateFormat ^:private iso8601 (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
(.setTimeZone iso8601 (TimeZone/getTimeZone "UTC"))

(defn iso8601-format
  "Format a date using ISO8601 format. Always UTC."
  [date]
  (.format iso8601 date))

(def ^:private ns-colors (ref {}))
(def ^:private next-ns-color (ref 1))
(defn- ns-color [ns]
  (when-not (@ns-colors ns)
    (dosync (alter ns-colors assoc ns (alter next-ns-color #(mod (inc %) 8)))))
  (@ns-colors ns))
(defn- color-str [i]
  (format "\u001b[%dm" i))

(defn pretty-log
  [{:keys [level throwable instant ns message ap-config]}]
  (let [base-color (case level
                     (:warn :error) 31
                     (:info) 36
                     0)]
    (locking pretty-log ; so we don't trip on ourselves
      (println (format "\r%s%s %s [%s%s%s] - %s%s%s"
                       (color-str base-color)
                       (iso8601-format instant)
                       (-> level name clojure.string/upper-case)
                       (color-str (+ 30 (ns-color ns))) ns (color-str base-color)
                       (or message "") (or (log/stacktrace throwable "\n") "")
                       (color-str 0)))
      (when-let [f (:after-msg ap-config)]
        (f)))))
