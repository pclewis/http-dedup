(ns http-dedup.buffer-manager-test
  (:require [clojure.test :as test :refer [deftest is use-fixtures]]
            [http-dedup.buffer-manager :refer :all]
            [http-dedup.async-utils :refer [get!!]]
            [clojure.core.async :as async])
  (:import [java.nio ByteBuffer]))

(defmethod test/report :begin-test-var [m]
  (test/with-test-out
    (println "Testing:" (-> m :var meta :name))))

(declare ^:dynamic *bufman*)

(defn buffer-manager-fixture [f]
  (binding [*bufman* (buffer-manager 2 16)]
    (f)
    (assert (= 2 (count @(.pool *bufman*))) "Buffers not released properly")
    (assert (empty? @(.waiting *bufman*)))) "Outstanding requests")

(use-fixtures :each buffer-manager-fixture)

(defn safe-request [bufman]
  (let [buf (get!! (.request *bufman*))]
    (assert (not (false? buf)))
    buf))

(deftest acquire-buffer
  (let [b1 (get!! (.request *bufman*))]
    (is (instance? ByteBuffer @b1))
    (.release! b1)))

(deftest wait-for-buffer
  (let [b1 (get!! (.request *bufman*))
        b2 (get!! (.request *bufman*))
        b3c (.request *bufman*)]
    (is (false? (get!! b3c false 100)))
    (.release! b1)
    (let [b3 (get!! b3c)]
      (is (not (false? b3)))
      (.release! b3))
    (.release! b2)))

(deftest copy-buffer
  (let [[b1 b2] (repeatedly #(safe-request *bufman*))
        b1-1 (.copy! b1)]
    (is (not (false? b2)))
    (is (not (identical? @b1 @b1-1)))
    (.put @b1 (.getBytes "test"))
    (.flip @b1)
    (.limit @b1-1 (.limit @b1))
    (is (= @b1 @b1-1))
    (doseq [b [b1 b2 b1-1]] (.release! b))))

(deftest return-copied-buffer
  (let [[b1 b2] (repeatedly #(get!! (.request *bufman*)))
        b1-1 (.copy! b1)
        b3c (.request *bufman*)]
    (.release! b1)
    (is (false? (get!! b3c false 10)))
    (.release! b1-1)
    (let [b3 (get!! b3c)]
      (assert (not (false? b3)))
      (is (identical? @b1 @b3))
      (.release! b3))
    (.release! b2)))

(deftest copy-copied-buffer
  (let [[b1 b2] (repeatedly #(safe-request *bufman*))
        b1-1 (.copy! b1)
        b1-1-1 (.copy! b1)
        b3c (.request *bufman*)]
    (.put @b1 (.getBytes "test"))
    (.flip @b1)
    (.limit @b1-1-1 (.limit @b1))
    (is (= @b1 @b1-1-1)) ;; same contents
    (.release! b1)
    (is (false? (get!! b3c false 50))) ;; not released yet
    (.release! b1-1)
    (is (false? (get!! b3c false 50))) ;; still not released
    (.release! b1-1-1)
    (let [b3 (get!! b3c)]
      (assert (not (false? b3)))
      (is (identical? @b1 @b3))
      (.release! b3))
    (.release! b2)))

(deftest cleanup
  (let [[b1 b2] (repeatedly #(get!! (.request *bufman*)))
        b3c (.request *bufman*)]
    (.put @b1 (.getBytes "test"))
    (.flip @b1)
    (.release! b1)
    (is (identical? @b1 @(doto (get!! b3c) (.release!))))
    (is (= (.capacity @b1) (.limit @b1)))
    (is (= 0 (.position @b1)))
    (.release! b2)))

(deftest closed-request-channel
  (let [c (async/chan)]
    (async/close! c)
    (.request *bufman* c)
    (let [[b1 b2] (repeatedly #(get!! (.request *bufman*) nil 50))]
      (is b1)
      (is b2)
      (doseq [b [b1 b2]] (.release! b)))))

(deftest closed-waiting-channel
  (let [[b1 b2] (repeatedly #(get!! (.request *bufman*)))
        b3c (.request *bufman*)
        b4c (.request *bufman*)]
    (async/close! b3c)
    (.release! b1)
    (let [b4 (get!! b4c)]
      (is (identical? @b1 @b4))
      (let [b5c (.request *bufman*)]
        (async/close! b5c)
        (.release! b2)
        (let [b6 (safe-request *bufman*)]
          (is (identical? @b2 @b6))
          (.release! b6)))
      (.release! b4))
    (.release! b2)))
