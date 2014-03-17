(ns http-dedup.socket-manager-tests
  (:require [clojure.test :refer :all]
            [http-dedup.socket-manager :as sockman]
            [http-dedup.util :refer [bytebuf-to-str str-to-bytebuf]]
            [clojure.core.async :as async :refer [>! >!! <! <!!]]))

(declare ^:dynamic *sockman*)

(defn sockman-fixture [f]
  (binding [*sockman* (sockman/socket-manager)]
    (try
      (f)
      (finally (async/close! *sockman*)))))

(use-fixtures :each sockman-fixture)

(def port 65432) ; should use an auto-assigned port, but the design doesn't really allow it

(deftest socket-manager-test
  (let [incoming-ch (sockman/listen *sockman* nil port)
        client->server (<!! (sockman/connect *sockman* nil port))
        server->client (<!! incoming-ch)
        [s<c s>c] (<!! (sockman/accept *sockman* server->client))
        [c<s c>s] (<!! (sockman/accept *sockman* client->server))]
    (>!! s>c (str-to-bytebuf "Hello world"))
    (is (= "Hello world" (bytebuf-to-str (<!! c<s))))))

(comment
  (run-tests 'http-dedup.socket-manager-tests)

  (taoensso.timbre/set-level! :trace)

  )
