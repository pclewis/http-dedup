(ns http-dedup.async-utils
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [taoensso.timbre :as log]))

(defmacro peek!
  ([chan] `(peek! ~chan 0))
  ([chan timeout]
     `(async/alt! ~chan ([v#] v#)
                  ~(if (= timeout 0) `:default `(async/timeout ~timeout)) nil)))

(defn peek!!
  ([chan] (peek!! chan 0))
  ([chan timeout]
     (async/alt!! chan ([v] v) (async/timeout timeout) nil)))


(defn get!!
  ([chan] (get!! chan false 0))
  ([chan none] (get!! chan none 0))
  ([chan none timeout]
     (if (= 0 timeout)
       (async/alt!! chan ([v] v) :default none)
       (async/alt!! chan ([v] v) (async/timeout timeout) none))))

(defmacro go-loop-<!
  "A loop that will take items from ch and bind them to binding.
   Will loop until the channel is closed."
  [ch binding & body]
  `(go-loop []
            (when-let [~binding (<! ~ch)]
              ~@body
              (recur))))

(defn drain
  ([ch] (drain ch identity))
  ([ch f & args]
     (if (fn? ch)
       (let [nch (async/chan)]
         (apply drain nch ch f args)
         nch)
       (let [fa (apply partial f args)]
         (go-loop-<! ch msg (fa msg))))))

(defmacro thread [& body]
  `(async/thread
    (try
      ~@body
      (catch Throwable t#
        (log/error t# "Thread exiting with exception")))))
