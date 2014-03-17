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
    (let [b1 (<!! (request *bufman*))]
      (is (instance? ByteBuffer b1))
      (return *bufman* b1)))

  (testing "Waiting for a buffer"
    (let [b1 (<!! (request *bufman*))
          b2 (<!! (request *bufman*))
          b3c (request *bufman*)]
      (is (nil? (peek!! b3c 100)))
      (return *bufman* b1)
      (is (identical? b1 (peek!! b3c 100)))
      (return *bufman* b1)
      (return *bufman* b2)))

  (testing "Copying a buffer"
    (let [[b1 b2] (repeatedly #(<!! (request *bufman*)))
          b1-1 (<!! (copy *bufman* b1))]
      (is (not (identical? b1 b1-1)))
      (.put b1 (.getBytes "test"))
      (.flip b1)
      (.limit b1-1 (.limit b1))
      (is (= b1 b1-1))
      (doseq [b [b1 b2 b1-1]] (return *bufman* b))))

  (testing "Buffer returned after copies returned"
    (let [[b1 b2](repeatedly #(<!! (request *bufman*)))
          b1-1 (<!! (copy *bufman* b1))
          b3c (request *bufman*)]
      (return *bufman* b1)
      (is (nil? (peek!! b3c 10)))
      (return *bufman* b1-1)
      (is (identical? b1 (<!! b3c)))
      (doseq [b [b1 b2]] (return *bufman* b))))

  (testing "Copying a copy"
    (let [[b1 b2] (repeatedly #(<!! (request *bufman*)))
          b1-1 (<!! (copy *bufman* b1))
          b1-1-1 (<!! (copy *bufman* b1))
          b3c (request *bufman*)]
      (.put b1 (.getBytes "test"))
      (.flip b1)
      (.limit b1-1-1 (.limit b1))
      (is (= b1 b1-1-1))
      (return *bufman* b1)
      (is (nil? (peek!! b3c 10)))
      (return *bufman* b1-1)
      (is (nil? (peek!! b3c 10)))
      (return *bufman* b1-1-1)
      (is (identical? b1 (<!! b3c)))
      (doseq [b [b1 b2]] (return *bufman* b))))

  (testing "Buffer cleanup"
    (let [[b1 b2] (repeatedly #(<!! (request *bufman*)))
          b3c (request *bufman*)]
      (.put b1 (.getBytes "test"))
      (.flip b1)
      (return *bufman* b1)
      (is (identical? b1 (<!! b3c)))
      (is (= (.capacity b1) (.limit b1)))
      (is (= 0 (.position b1)))
      (doseq [b [b1 b2]] (return *bufman* b)))))


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
