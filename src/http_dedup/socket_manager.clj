(ns http-dedup.socket-manager
  (:import [java.nio.channels SocketChannel ServerSocketChannel]
           [java.net InetAddress InetSocketAddress])
  (:require [http-dedup
             [async-utils :refer :all]
             [select :as select]
             [buffer-manager :as bufman]]
            [clojure.core.async :as async :refer [go go-loop >! <!]]))

(defasync socket-manager [select bufman]
  (create [] {:select (select/select)
              :bufman (bufman/buffer-manager 16 32767)})

  (fn writer [socket]
    (let [inch (async/chan)]
      (go-loop []
               (when-let [buf (<! inch)]
                 (while (.hasRemaining buf)
                   (if (<! (select/write select socket))
                     (.write socket buf)
                     (.clear buf)))
                 (bufman/return bufman buf)
                 (recur)))
      inch))

  (fn reader [socket]
    (let [outch (async/chan)]
      (go-loop []
               (when (<! (select/read select socket))
                 (let [buf (<! (bufman/request bufman))
                       n-read (.read socket buf)]
                   (if (>= 0 n-read)
                     (bufman/return bufman buf) ;FIXME close socket..
                     (do (>! outch buf)
                         (recur))))))
      outch))

  (fn connector [socket]
    (let [outch (async/chan)]
      (go-loop []
               (when (<! (select/connect select socket))
                 (if (.finishConnect socket)
                   (>! outch socket)
                   (recur))))
      outch))

  (fn acceptor [socket]
    (let [outch (async/chan)]
      (go-loop []
               (when (<! (select/accept select socket))
                 (let [new-sock (.accept socket)]
                   (>! outch new-sock)
                   (recur))))
      outch))

  (connect
   [out host port]
   (let [socket (doto (SocketChannel/open) (.configureBlocking false))]
     (.connect socket (InetSocketAddress. (InetAddress/getByName host) port))
     (async/pipe (connector socket) out)))

  (listen
   [out host port]
   (let [socket (doto (ServerSocketChannel/open) (.configureBlocking false))]
     (.bind socket (InetSocketAddress. (InetAddress/getByName host) port))
     (async/pipe (acceptor socket) out)
     nil))

  (accept
   [out socket]
   (let [rch (reader socket)
         wch (writer socket)]
     (>! out [rch wch])))

  (return-buffer [buf] (bufman/return bufman buf))
  (copy-buffer [out buf] (async/pipe (bufman/copy bufman buf) out)))
