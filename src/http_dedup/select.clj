(ns http-dedup.select
  (:import [java.nio.channels Selector SelectionKey])
  (:require [clojure.core.async :as async :refer [go go-loop >! <! >!! <!!]]
            [http-dedup.async-utils :refer [defasync thread get!!]]
            [http-dedup.util :refer [bit-seq]]
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
(defn- selector-thread [selector manager]
  (thread
   (loop []
     (when (< 0 (.select selector))
       (dispatch-events selector))

     (when-let [messages (<!! (get-messages manager))]
       (when (seq messages)
         (doseq [msg messages]
           (handle-message selector msg)))
       (recur)))

   (log/info "Select thread exiting")
   (doseq [k (.keys selector)]
     (.cancel k)
     (.close (.channel k)))))

(defasync select [selector selector-thread mailbox this]
  (create [] (let [selector (Selector/open)]
               {:selector selector
                :selector-thread (selector-thread selector this)
                :mailbox []}))

  (destroy (.wakeup selector))

  ;; there's a hard limit of 1024 parked puts on a channel.
  ;; collect messages in a vector, so callers park on their own channel and not ours.
  (fn sub [op socket out]
    (when-not (seq mailbox)
      (.wakeup selector))
    {:mailbox (conj mailbox [op socket out])})

  (get-messages [out] (>! out mailbox)
                {:mailbox []})

  (read [out socket] (sub SelectionKey/OP_READ socket out))
  (write [out socket] (sub SelectionKey/OP_WRITE socket out))
  (accept [out socket] (sub SelectionKey/OP_ACCEPT socket out))
  (connect [out socket] (sub SelectionKey/OP_CONNECT socket out))
  (close [out socket] (sub 0 socket out))
  (debug-select-thread [] (sub :debug-state nil nil)))
