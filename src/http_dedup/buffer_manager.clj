(ns http-dedup.buffer-manager
  (:import [java.nio ByteBuffer])
  (:require [http-dedup.actor :refer [defactor run-actor]]
            [clojure.core.async :as async :refer [go <! >! put!]]
            [taoensso.timbre :as log]))

(defactor BufferManager
  (request [])
  (copy [buf])
  (return [buf])
  (debug []))

(defn- buf-id [o] (System/identityHashCode o))
(defn- parent [pm buf] (or (get pm (buf-id buf)) buf))

(defrecord Bufman [pool pending-requests id-map ref-map parent-map]
  BufferManager
  (request [this out]
    (if-let [buf (first pool)]
      (let [id (buf-id buf)]
        (if (put! out buf)
          (do (log/trace "request: giving out:" id)
              {:id-map (assoc id-map id buf)
               :ref-map (assoc ref-map id #{id})
               :pool (rest pool)})
          (log/warn "request: requester went away, keeping" id)))
      (do (log/warn "request: no buffers available, queuing a request")
          {:pending-requests (conj pending-requests out)})))

  (copy [this out buf]
    (let [src (parent parent-map buf)
          src-id (buf-id src)
          dst (.asReadOnlyBuffer ^ByteBuffer buf)
          dst-id (buf-id dst)
          refs (ref-map src-id)]
      (when-not refs (log/warn "copy: request to copy a buffer with no outstanding refs!"
                               {:buf buf :src src :src-id src-id :ref-map ref-map}))
      (if (put! out dst)
        (do (log/trace "copy:" src-id "->" dst-id)
            (when refs
              {:ref-map (assoc ref-map src-id (conj refs dst-id))
               :parent-map (assoc parent-map dst-id src)}))
        (log/warn "copy: requester went away, discarding copy"))) )

  (return [this out buf]
    (let [^ByteBuffer src (parent parent-map buf)
          src-id (buf-id src)
          id (buf-id buf)]
      (log/trace "return:" id "(clone of" src-id ")" )
      (merge
       {:parent-map (dissoc parent-map id)} ; whatever else may happen, copies die here
       (if-let [refs (disj (ref-map src-id) id)]
         (if (empty? refs)
           (do (log/trace "return: no more references to" src-id)
               (.clear src)
               (or (loop [pr pending-requests]
                     (when (not-empty pr)
                       (if (put! (peek pr) src)
                         (do (log/trace "return: gave" src-id "to waiting request")
                             {:ref-map (assoc ref-map src-id #{src-id})
                              :pending-requests (pop pending-requests)})
                         (do (log/trace "return: pending requester went away, removing from queue")
                             (recur (pop pr))))))
                   {:pending-requests (empty pending-requests) ; only get here if no prs
                    :pool (cons src pool)
                    :ref-map (dissoc ref-map src-id)}))
           {:ref-map (assoc ref-map src-id refs)})
         (log/debug "return: buffer doesn't belong to us, discarding"))))))

(defn buffer-manager [max-buffers buffer-size]
  (let [actor (BufferManagerActor. (async/chan))]
    (run-actor actor (Bufman. (repeatedly max-buffers #(ByteBuffer/allocate buffer-size))
                              [] {} {} {}))
    actor))
