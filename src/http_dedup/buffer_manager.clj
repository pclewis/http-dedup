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
           (if-let [buf (first pool)]
             (let [id (buf-id buf)]
               (if (>! out buf)
                 (do (log/trace "request: giving out:" id)
                     {:id-map (assoc id-map id buf)
                      :ref-map (assoc ref-map id #{id})
                      :pool (rest pool)})
                 (log/warn "request: requester went away, keeping" id)))

             (do (log/warn "request: no buffers available, queuing a request")
                 {:pending-requests (conj pending-requests out)})))

  (copy [out buf]
        (let [src (parent buf)
              src-id (buf-id src)
              dst (.asReadOnlyBuffer buf)
              dst-id (buf-id dst)
              refs (ref-map src-id)]
          (when-not refs (log/warn "copy: request to copy a buffer with no outstanding refs!"
                                   {:buf buf :src src :src-id src-id :ref-map ref-map}))
          (if (>! out dst)
            (do (log/trace "copy:" src-id "->" dst-id)
                (when refs
                  {:ref-map (assoc ref-map src-id (conj refs dst-id))
                   :parent-map (assoc parent-map dst-id src)}))
            (log/warn "copy: requester went away, discarding copy"))))

  (return [buf]
          (let [src (parent buf)
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
                             (if (>! (peek pr) src)
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
