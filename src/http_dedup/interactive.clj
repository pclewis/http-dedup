(ns http-dedup.interactive
  "Interactive command-line mode"
  (:import [jline.console ConsoleReader]
           [jline.console.completer ArgumentCompleter StringsCompleter])
  (:require [taoensso.timbre :as log]
            [clojure.core.async :as async]))

(defn session [server]
  (let [reader (ConsoleReader.)]
    (log/set-config! [:shared-appender-config :after-msg] #(doto reader .drawLine .flush))
    (.addCompleter reader (ArgumentCompleter.
                           [(StringsCompleter. (map str '(quit bufman sockman atc select
                                                               repl level mute unmute)))
                            (StringsCompleter. (map str (all-ns)))]))
    (loop []
      (when-let [line (.readLine reader "http-dedup> ")]
        (let [[cmd & args] (clojure.string/split line #"\s+")]
          (case cmd
            ("quit" "") nil
            ("bufman" "sockman" "atc" "select") (async/>!! server (into [(keyword cmd)] args))
            ("repl") (clojure.main/repl :read (fn [_ breaker]
                                                (if-let [line (.readLine reader "repl> ")]
                                                  (read-string line)
                                                  breaker))
                                        :prompt #())
            ("level") (log/set-level! (keyword (first args)))
            ("mute") (log/set-config! [:ns-blacklist]
                                      (conj (:ns-blacklist @log/config) (first args)))
            ("unmute") (log/set-config! [:ns-blacklist]
                                        (remove (set args) (:ns-blacklist @log/config)))
            (println "Unrecognized command: " cmd))
          (when-not (= cmd "quit") (recur)))))
    (.shutdown reader)
    (log/set-config! [:shared-appender-config :after-msg] nil)))
