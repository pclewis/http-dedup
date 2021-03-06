(ns http-dedup.socket-manager-tests
  (:require [clojure.test :refer :all]
            [http-dedup.socket-manager :as sockman]
            [http-dedup.buffer-manager :as bufman]
            [http-dedup.select :as select]
            [http-dedup.util :refer [bytebuf-to-str str-to-bytebuf]]
            [clojure.core.async :as async :refer [>! >!! <! <!!]]))

(declare ^:dynamic *sockman*)

(defn sockman-fixture [f]
  (binding [*sockman* (sockman/socket-manager (select/select)
                                              (bufman/buffer-manager 16 1024))]
    (try
      (f)
      (finally (async/close! *sockman*)))))

(use-fixtures :each sockman-fixture)

(deftest socket-manager-test
  (let [port (+ (- 0x10000 1000) (rand-int 1000))
        ; ^ sucks but design doesn't allow for a real ephemeral port
        incoming-ch (sockman/listen *sockman* nil port)
        _ (<!! (async/timeout 5)) ; sometimes listen isn't ready yet
        outgoing-ch (sockman/connect *sockman* nil port)
        server (<!! incoming-ch)
        client (<!! outgoing-ch)]
    (>!! (:write server) (str-to-bytebuf "Hello world"))
    (is (= "Hello world" (bytebuf-to-str @(<!! (:read client)))))))
