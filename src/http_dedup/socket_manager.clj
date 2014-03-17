(ns http-dedup.socket-manager
  (:import [java.nio.channels SocketChannel ServerSocketChannel]
           [java.net InetAddress InetSocketAddress])
  (:require [http-dedup
             [async-utils :refer :all]
             [select :as select]
             [buffer-manager :as bufman]]
            [clojure.core.async :as async :refer [go go-loop >! <!]]
            [taoensso.timbre :as log]))

(defasync socket-manager [select bufman]
  (create [] {:select (select/select)
              :bufman (bufman/buffer-manager 16 32767)})

  (destroy (async/close! select)
           (async/close! bufman))

  (fn writer [socket]
    (let [inch (async/chan)]
      (go-loop []
               (if-let [buf (<! inch)]
                 (do (log/trace "Received a buffer to write" buf)
                     (while (.hasRemaining buf)
                       (if (<! (select/write select socket))
                         (try (.write socket buf)
                              (catch java.nio.channels.ClosedChannelException _
                                (.position buf (.limit buf))))
                         (.position buf (.limit buf))))
                     (bufman/return bufman buf)
                     (recur))
                 (do (log/debug "write: closing connection" socket)
                     (select/close select socket))))
      inch))

  (fn reader [socket wch]
    (let [outch (async/chan)]
      (go-loop []
               (when (<! (select/read select socket))
                 (let [buf (<! (bufman/request bufman))
                       n-read (try (.read socket buf)
                                   (catch java.nio.channels.ClosedChannelException _
                                     -1))]
                   (log/trace "Received" n-read "bytes on socket" buf)
                   (if (> 0 n-read)
                     (do (log/debug "read: closing connection" socket)
                         (bufman/return bufman buf)
                         (async/close! outch)
                         (async/close! wch)
                         (select/close select socket))
                     (do (.flip buf)
                         (>! outch buf)
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
                   (.configureBlocking new-sock false)
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
     (.bind (.socket socket) (InetSocketAddress. (InetAddress/getByName host) port))
     (async/pipe (acceptor socket) out)
     nil))

  (accept
   [out socket]
   (let [wch (writer socket)
         rch (reader socket wch)]
     (>! out [rch wch])))

  (return-buffer [buf] (bufman/return bufman buf))
  (copy-buffer [out buf] (async/pipe (bufman/copy bufman buf) out)))
