(ns http-dedup.select-tests
  (:require [clojure.test :refer :all]
            [http-dedup.select :as select]
            [http-dedup.async-utils :refer :all]
            [clojure.core.async :as async :refer [>! >!! <! <!!]])
  (:import [java.nio.channels SocketChannel ServerSocketChannel]
           [java.net InetSocketAddress InetAddress]))

(declare ^:dynamic *select*)

(defn select-fixture [f]
  (binding [*select* (select/select)]
    (f)
    (async/close! *select*)))

(use-fixtures :each select-fixture)

(defn make-server []
  (doto (ServerSocketChannel/open)
    (.configureBlocking false)
    (.bind (InetSocketAddress. (InetAddress/getByName nil) 0))))

(defn make-client [server]
  (doto (SocketChannel/open)
    (.configureBlocking false)
    (.connect (.getLocalSocketAddress (.socket server)))))

(deftest select-test-accept-connect
  (let [server (make-server)
        accept-ch (select/accept *select* server)]
    (is (false? (get!! accept-ch)))
    (let [client (make-client server)
          conn-ch (select/connect *select* client)]
      (is (get!! conn-ch false 500))
      (is (false? (get!! conn-ch false 50)))
      (is (get!! accept-ch false 500))
      (is (false? (get!! accept-ch false 50))))))




(comment
  (remove-ns 'http-dedup.select-tests)

  (run-tests 'http-dedup.select-tests)

  )
