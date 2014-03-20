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
  (create [select bufman] {:select (or select (select/select))
                           :bufman (or bufman (bufman/buffer-manager 16 32767))})

  (destroy (async/close! select)
           (async/close! bufman))

  (fn writer [socket]
    (let [inch (async/chan)]
      (go-loop []
               (if-let [buf (<! inch)]
                 (do (log/trace "Received a buffer to write" buf)
                     (while (.hasRemaining buf)
                       (when-not (and (<! (select/write select socket))
                                      (< 0 (try (.write socket buf)
                                                (catch java.nio.channels.ClosedChannelException _ -1))))
                         (log/debug "write: socket is closed, discarding buffer")
                         (.limit buf 0)))
                     (bufman/return bufman buf)
                     (recur))
                 (do (log/debug "write: closing connection" socket)
                     (select/close select socket))))
      inch))

  (fn reader [socket wch]
    (let [outch (async/chan)]
      (go-loop []
               (if (<! (select/read select socket))
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
                         (or (>! outch buf)
                             (bufman/return bufman buf))
                         (recur))))
                 (do (log/debug "read: connection closed")
                     (async/close! outch)
                     (async/close! wch))))
      outch))

  (fn connector [socket outch]
    (go-loop []
             (when (<! (select/connect select socket))
               (if (.finishConnect socket)
                 (>! outch socket)
                 (recur)))))

  (fn acceptor [socket outch]
    (go-loop []
             (when (<! (select/accept select socket))
               (let [new-sock (.accept socket)]
                 (.configureBlocking new-sock false)
                 (>! outch new-sock)
                 (recur)))))

  (connect
   [out host port]
   (let [socket (doto (SocketChannel/open) (.configureBlocking false))]
     (.connect socket (InetSocketAddress. (InetAddress/getByName host) port))
     (connector socket out)))

  (listen
   [out host port]
   (let [socket (doto (ServerSocketChannel/open) (.configureBlocking false))]
     (.bind (.socket socket) (InetSocketAddress. (InetAddress/getByName host) port))
     (acceptor socket out)
     nil))

  (accept
   [out socket]
   (let [wch (writer socket)
         rch (reader socket wch)]
     (>! out [rch wch])))

  (return-buffer [buf] (<! (bufman/return bufman buf)))
  (copy-buffer [out buf] (<! (bufman/copy bufman out buf))))
