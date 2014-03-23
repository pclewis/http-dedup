(ns http-dedup.select
  (:import [java.nio.channels Selector SelectionKey])
  (:require [clojure.core.async :as async :refer [go go-loop >! <! >!! <!!]]
            [http-dedup.async-utils :refer [thread get!!]]
            [http-dedup.util :refer [bit-seq pre-swap!]]
            [taoensso.timbre :as log])
  (:refer-clojure :exclude [read write]))

(defn- op-to-kw [op]
  (condp = op ; case never matches, for some reason
    SelectionKey/OP_WRITE :write
    SelectionKey/OP_READ :read
    SelectionKey/OP_ACCEPT :accept
    SelectionKey/OP_CONNECT :connect
    0 :close))

(defn- dispatch-events [selector]
  (let [keys (.selectedKeys selector)]
    (log/trace "Selected some keys" keys)
    (doseq [key keys :let [ops (.readyOps key)
                           opseq (bit-seq ops)
                           receivers (.attachment key)]]
      (.interestOps key (bit-and-not (.interestOps key) ops))
      (.attach key (apply dissoc (.attachment key) opseq))
      (doseq [op opseq
              r (get receivers op)]
        (log/trace "channel" (.channel key) "received op" (op-to-kw op))
        (go (>! r key))))
    (.clear keys)))

(defn- debug-state [selector]
  (log/debug
   (map (fn [k] (str (.channel k) "->"
                    (clojure.string/join
                     "," (map #(str (op-to-kw (key %)) "->" (val %))
                              (.attachment k)))))
        (.keys selector))))

(defn- handle-message [selector [op ch rch]]
  (if (= op :debug-state)
    (debug-state selector)
    (let [sk (.keyFor ch selector)]
      (log/trace "selector: got message: " [(op-to-kw op) ch rch])
      (if-not (.isOpen ch)
        (do (log/trace "request on closed channel" ch)
            (async/close! rch))
        (if (= 0 op)
          (do (log/debug "Closing connection" ch)
              (when sk
                (when-let [a (.attachment sk)]
                  (doseq [[_ achs] a
                          ach achs]
                    (async/close! ach)))
                (.cancel sk))
              (.close ch))
          (if sk
            (if (.isValid sk)
              (do (.interestOps sk (bit-or (.interestOps sk) op))
                  (.attach sk (merge-with concat (.attachment sk) {op [rch]})))
              (do (log/error "SelectionKey became invalid.." ch)
                  (async/close! rch)))
            (.register ch selector op {op [rch]})))))))

(declare get-messages)
(defn- selector-thread [selector mailbox]
  (thread
   (loop []
     (when (< 0 (.select selector))
       (dispatch-events selector))

     (when-let [messages (pre-swap! mailbox empty)]
       (when (seq messages)
         (doseq [msg messages]
           (handle-message selector msg)))
       (recur)))

   (log/info "Select thread exiting")
   (doseq [k (.keys selector)]
     (.cancel k)
     (.close (.channel k)))))

;; Don't do this agent-style because it is the busiest part of the system.
;; If 2000 sockets want to write or close at the same time, core.async would
;; hit the limit of 1024 parked puts on a single channel.
;; We can still present the same API, however.
(defn select []
  (let [selector (Selector/open)
        mailbox (atom [])]
    {:selector selector
     :thread (selector-thread selector mailbox)
     :mailbox mailbox}))

(defn- sub [select op socket out]
  (let [c (count (swap! (:mailbox select) conj [op socket out]))]
    (when (= 1 c)
      (.wakeup (:selector select)))))

(defmacro subfns [& ops]
  (cons 'do
        (for [op ops]
          `(defn ~op
             (~'[select socket]
              (let [ch# (async/chan)]
                (~op ~'select ch# ~'socket)
                ch#))
             (~'[select out socket]
              (sub ~'select ~(symbol (str "SelectionKey/OP_" (clojure.string/upper-case (str op))))
                   ~'socket ~'out))))))

(subfns read write accept connect)

(defn close
  ([select socket] (let [ch (async/chan)]
                     (close select ch socket)
                     ch))
  ([select out socket] (sub select 0 socket out)))

(defn debug-select-thread [select]
  (sub select :debug-state nil nil))

(defn shutdown! [select]
  (let [messages (pre-swap! (:mailbox select) (fn [_] nil))]
    (doseq [[_ _ ch] messages]
      (async/close! ch))))
