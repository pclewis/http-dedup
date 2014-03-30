(ns http-dedup.buffer-manager
  (:import [java.nio ByteBuffer]
           [clojure.lang PersistentQueue])
  (:require [http-dedup.actor :refer [defactor run-actor]]
            [http-dedup.util :refer [dequeue!]]
            [clojure.core.async :as async :refer [chan put!]]
            [taoensso.timbre :as log]))

(defprotocol IArcByteBuffer
  (unwrap [this])
  (copy! [this])
  (release! [this]))

(defprotocol IBufferManager
  (request [this] [this out])
  (return [this buffer]))

;; For convenience, allow ByteBuffers to be treated as ArcByteBuffers
(extend-type ByteBuffer
  IArcByteBuffer
  (unwrap [this] this)
  (copy! [this] (.asReadOnlyBuffer this))
  (release! [this] nil))

(deftype ArcByteBuffer [parent
                        ^ByteBuffer byte-buffer
                        manager
                        n-refs]
  clojure.lang.IDeref
  (deref [this] byte-buffer)

  IArcByteBuffer
  (unwrap [this] byte-buffer)

  (copy! [this]
    (if parent
      (copy! parent)
      (do (swap! n-refs inc)
          (ArcByteBuffer. this (.asReadOnlyBuffer byte-buffer) nil nil))))

  (release! [_]
    (if parent
      (release! parent)
      (when (zero? (swap! n-refs dec))
        (.clear byte-buffer)
        (return manager byte-buffer)))))

(deftype BufferManager [pool waiting]
  IBufferManager
  (request [this]
    (request this (chan)))
  (request [this out]
    (if-let [buf (dequeue! pool)]
      (when-not (put! out (ArcByteBuffer. nil buf this (atom 1)))
        (swap! pool conj buf))
      (swap! waiting conj out))
    out)
  (return [this buf]
    (loop []
      (if-let [ch (dequeue! waiting)]
        (when-not (put! ch (ArcByteBuffer. nil buf this (atom 1)))
          (recur))
        (swap! pool conj buf)))))

(defn buffer-manager [max-buffers buffer-size]
  (BufferManager.
   (atom (apply list
          (repeatedly max-buffers #(ByteBuffer/allocateDirect buffer-size))))
   (atom PersistentQueue/EMPTY)))
