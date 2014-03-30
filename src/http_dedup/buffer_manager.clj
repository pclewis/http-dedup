(ns http-dedup.buffer-manager
  (:import [java.nio ByteBuffer]
           [clojure.lang PersistentQueue])
  (:require [http-dedup.actor :refer [defactor run-actor]]
            [http-dedup.util :refer [dequeue!]]
            [clojure.core.async :as async :refer [chan put!]]
            [taoensso.timbre :as log]))

(definterface IArcByteBuffer
  (copy_BANG_ [])
  (^void release_BANG_ []))

(definterface IBufferManager
  (request [])
  (request [out])
  (^void return [^java.nio.ByteBuffer buffer]))

(deftype ArcByteBuffer [^IArcByteBuffer parent
                        ^ByteBuffer byte-buffer
                        ^IBufferManager manager
                        n-refs]
  clojure.lang.IDeref
  (deref [this] byte-buffer)

  IArcByteBuffer
  (copy! [this]
    (if parent
      (.copy! parent)
      (do (swap! n-refs inc)
          (ArcByteBuffer. this (.asReadOnlyBuffer byte-buffer) nil nil))))

  (release! [_]
    (if parent
      (.release! parent)
      (when (zero? (swap! n-refs dec))
        (.clear byte-buffer)
        (.return manager byte-buffer)))))

(deftype BufferManager [pool waiting]
  IBufferManager
  (request [this]
    (.request this (chan)))
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
