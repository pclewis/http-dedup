(ns http-dedup.select
  (:import [java.nio.channels Selector SelectionKey])
  (:require [clojure.core.async :as async :refer [go go-loop >! <!]]
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
               (log/trace "channel" (.channel key) "received op" (op-to-kw op))
               (go (>! r key))))
           (.clear keys)))

       (let [closed?
             (loop [msg (get!! subch)] ; false if empty, nil if closed
               (if-not msg
                 (nil? msg)
                 (if (= [:debug-state nil nil] msg)
                   (log/debug
                    (map (fn [k] (str (.channel k) "->"
                                     (clojure.string/join
                                      "," (map #(str (op-to-kw (key %)) "->" (val %))
                                               (.attachment k)))))
                         (.keys selector)))
                   (let [[op ch rch] msg
                         sk (.keyFor ch selector)]
                     (log/trace "selector: got message: " [(op-to-kw op) ch rch] )
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
                           (.register ch selector op {op [rch]}))))
                     (recur (get!! subch))))))]
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

  (destroy (log/debug "select: killing select thread")
           (async/close! subch)
           (.wakeup selector))

  (fn sub [op socket out]
    (go
     (>! subch [op socket out])
     (.wakeup selector)))

  (read [out socket] (sub SelectionKey/OP_READ socket out))
  (write [out socket] (sub SelectionKey/OP_WRITE socket out))
  (accept [out socket] (sub SelectionKey/OP_ACCEPT socket out))
  (connect [out socket] (sub SelectionKey/OP_CONNECT socket out))
  (close [out socket] (sub 0 socket out))
  (debug-select-thread [] (sub :debug-state nil nil)))
