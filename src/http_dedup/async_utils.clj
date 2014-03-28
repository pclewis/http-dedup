(ns http-dedup.async-utils
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.core.async.impl.protocols :as async-protos]
            [taoensso.timbre :as log])
  (:refer-clojure :exclude [concat]))

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

(defmacro get!
  ([chan] `(get! ~chan false 0))
  ([chan none] `(get! ~chan ~none 0))
  ([chan none timeout]
     (if (= 0 timeout)
       `(async/alt! ~chan ([v#] v#) :default ~none)
       `(async/alt! ~chan ([v#] v#) (async/timeout ~timeout) ~none))))

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

(defn concat
  "Return a channel that contains everything from each supplied channel, in
   order, and closes when all channels are exhausted."
  [& cs]
  (let [ch (async/chan)]
    (go
      (doseq [c cs]
        (loop []
          (when-let [v (<! c)]
            (>! ch v)
            (recur))))
      (async/close! ch))
    ch))

(defn readable?
  "Return true if ch is a channel or can be read from like one."
  [ch] (satisfies? async-protos/ReadPort ch))
