(ns http-dedup.buffer-manager
  (:import [java.nio ByteBuffer])
  (:require [http-dedup.async-utils :refer [defasync]]
            [clojure.core.async :as async :refer [<! >!]]
            [taoensso.timbre :as log]))

;(clojure.pprint/pprint (macroexpand (quote)))
(defasync buffer-manager
  [pool id-map parent-map ref-map pending-requests]
  (create [max-buffers buffer-size]
          {:pool (repeatedly max-buffers #(ByteBuffer/allocate buffer-size))
           :pending-requests []
           :id-map {}
           :parent-map {}
           :ref-map {}})

  (fn buf-id [buf] (System/identityHashCode buf))
  (fn parent [buf] (or (get parent-map (buf-id buf)) buf))

  (request [out]
           (let [buf (first pool)]
             (if buf
               (do
                 (log/trace "Giving out" (buf-id buf))
                 (let [id (buf-id buf)]
                   (>! out buf)
                   {:id-map (assoc id-map id buf)
                    :ref-map (assoc ref-map id #{id})
                    :pool (rest pool)}))
               (do
                 (log/debug "No buffers available, placing request in queue")
                 {:pending-requests (conj pending-requests out)}))))

  (copy [out buf]
        (let [src (parent buf)
              src-id (buf-id src)
              dst (.asReadOnlyBuffer buf)
              dst-id (buf-id dst)]
          (log/trace "Cloning" src-id "->" dst-id)
          (>! out dst)
          (when-let [refs (ref-map src-id)]
            {:ref-map (assoc ref-map src-id (conj refs dst-id))
             :parent-map (assoc parent-map dst-id src)})))

  (return [buf]
          (let [src (parent buf)
                src-id (buf-id src)
                id (buf-id buf)]
            (log/trace "Buffer returned:" id " (clone of" src-id ")" )
            (when-let [refs (disj (ref-map src-id) id)]
              (if (empty? refs)
                (do
                  (.clear src)
                  (if (empty? pending-requests)
                    {:pool (cons src pool)
                     :ref-map (dissoc ref-map src-id)}
                    (do (>! (peek pending-requests) src)
                        {:ref-map (assoc ref-map src-id #{src-id})
                         :pending-requests (pop pending-requests)})))
                {:ref-map (assoc ref-map src-id refs)})))))
