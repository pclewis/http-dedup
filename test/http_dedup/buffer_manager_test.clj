(ns http-dedup.buffer-manager-test
  (:require [clojure.test :refer :all]
            [http-dedup.buffer-manager :refer :all]
            [http-dedup.async-utils :refer :all]
            [clojure.core.async :as async :refer [>! >!! <! <!!]])
  (:import [java.nio ByteBuffer]))

(declare ^:dynamic *bufman*)

(defn buffer-manager-fixture [f]
  (binding [*bufman* (buffer-manager 2 16)]
    (f)
    (async/close! *bufman*)))

(use-fixtures :each buffer-manager-fixture)

(deftest buffer-manager-test
  (testing "Acquiring a buffer"
    (let [b1 (peek!! (request!! *bufman*) 100)]
      (is (instance? ByteBuffer b1))
      (return!! *bufman* b1)))
  (testing "Waiting for a buffer"
    (let [b1 (peek!! (request!! *bufman*))
          b2 (peek!! (request!! *bufman*))
          b3c (request!! *bufman*)]
      (is (nil? (peek!! b3c)))
      (return!! *bufman* b1)
      (is (= b1 (peek!! b3c)))
      (return!! *bufman* b1)
      (return!! *bufman* b2)))
  (testing "Copying a buffer"
    (let [[b1 b2] (repeatedly #(peek!! (request!! *bufman*)))
          b1-1 (copy!! *bufman* b1)]
      (.write b1 (.getBytes "test"))
      (.limit b1-1 (.limit b1))
      (is (= b1 b1-1))
      (doseq [b [b1 b2 b1-1]] (return!! *bufman* b)))))


(comment
  (run-tests 'http-dedup.buffer-manager-test)

  (let [q (async/chan)]
    (async/alt!! q nil :default 1 ))

  (let [bm (buffer-manager 0 1)
        req (request bm)]
    (return bm nil)
    (println "-----" (async/alt!! req :bad :default :good))
    (Thread/sleep 1000)
    (async/close! bm))

  (clojure.repl/source return)

  )
