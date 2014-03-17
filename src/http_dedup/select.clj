(ns http-dedup.select
  (:import [java.nio.channels Selector SelectionKey])
  (:require [clojure.core.async :as async :refer [go go-loop >! <!]]
            [http-dedup.async-utils :refer [defasync thread get!!]]
            [http-dedup.util :refer [bit-seq]]
            [taoensso.timbre :as log])
  (:refer-clojure :exclude [read write]))

(defn selector-thread [selector]
  (let [subch (async/chan 1)] ; buffer 1 msg so someone can write and then wake us up
    (thread
     (loop []
       (when (< 0 (.select selector))
         (let [keys (.selectedKeys selector)]
           (log/trace "Selected some keys" keys)
           (doseq [key keys :let [ops (.readyOps key)
                                  opseq (bit-seq ops)
                                  receivers (.attachment key)]]
             (.interestOps key (bit-and-not (.interestOps key) ops))
             (.attach key (apply dissoc (.attachment key) opseq))
             (doseq [op opseq
                     r (get receivers op)]
               (log/trace "Key" key "received op" op)
               (go (>! r key))))
           (.clear keys)))

       (let [closed?
             (loop [msg (get!! subch)] ; false if empty, nil if closed
               (log/debug "Got message: " msg)
               (if msg
                 (let [[op ch rch] msg
                       sk (.keyFor ch selector)]
                   (if sk
                     (do (.interestOps sk (bit-or (.interestOps sk) op))
                         (.attach sk (merge-with concat (.attachment sk) {op [rch]})))
                     (.register ch selector op {op [rch]}))
                   (recur (get!! subch)))
                 (nil? msg)))]
         (when-not closed?
           (recur))))
     (log/info "Select thread exiting")
     (doseq [k (.keys selector)]
       (.cancel k)
       (.close (.channel k))))
    subch))

(defasync select [subch selector]
  (create [] (let [selector (Selector/open)]
               {:selector selector
                :subch (selector-thread selector)}))

  (destroy (async/close! subch)
           (.wakeup selector))

  (fn sub [op socket out]
    (go
     (>! subch [op socket out])
     (.wakeup selector)))

  (read [out socket] (sub SelectionKey/OP_READ socket out))
  (write [out socket] (sub SelectionKey/OP_WRITE socket out))
  (accept [out socket] (sub SelectionKey/OP_ACCEPT socket out))
  (connect [out socket] (sub SelectionKey/OP_CONNECT socket out)))
