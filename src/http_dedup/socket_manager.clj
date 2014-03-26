(ns http-dedup.socket-manager
  (:import [java.nio.channels SocketChannel ServerSocketChannel]
           [java.net InetAddress InetSocketAddress])
  (:require [http-dedup
             [async-utils :refer :all]
             [select :as select]
             [buffer-manager :as bufman]
             [actor :refer [defactor run-actor]]]
            [clojure.core.async :as async :refer [go go-loop >! <!]]
            [taoensso.timbre :as log]))

(defactor SocketManager
  (connect [host port])
  (listen [host port])
  (accept [socket])
  (connect-and-accept [host port])
  (return-buffer [buf])
  (copy-buffer [buf]))

(defn- write-socket [socket buf]
  (try (.write socket buf)
       (catch java.io.IOException _ -1))) ; ClosedChannelException, broken pipe, etc

(defn- read-socket [socket buf]
  (try (.read socket buf)
       (catch java.io.IOException _ -1)))

(defn- finish-connect [socket outch]
  (try (when (.finishConnect socket)
         (async/put! outch socket)
         true)
       (catch java.io.IOException e ; SocketException, ConnectException
         (log/error "finishConnect failed:" socket ":" (.getMessage e))
         (async/close! outch)
         true)))

(defn- writer [{:keys [bufman select]} socket]
  (let [inch (async/chan 64)]
    (go-loop []
             (if-let [buf (<! inch)]
               (do (log/trace socket "received a buffer to write" buf)
                   (while (.hasRemaining buf)
                     (when-not (and (<! (select/write select socket))
                                    (< 0 (write-socket socket buf)))
                       (log/debug "write: socket is closed, discarding buffer:" socket)
                       (.limit buf 0)
                       (async/close! inch)))
                   (bufman/return bufman buf)
                   (recur))
               (do (log/debug "write: closing connection" socket)
                   (select/close select socket))))
    inch))

(defn- reader [{:keys [bufman select]} socket wch]
  (let [outch (async/chan 64)]
    (go-loop []
             (if (<! (select/read select socket))
               (let [buf (<! (bufman/request bufman))
                     n-read (read-socket socket buf)]
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

(defn- connector [{:keys [select]} socket outch]
  (go-loop []
           (when (<! (select/connect select socket))
             (if-not (finish-connect socket outch)
               (recur)))))

(defn- acceptor [{:keys [select]} socket outch]
  (go-loop []
           (if (<! (select/accept select socket))
             (let [new-sock (.accept socket)]
               (.configureBlocking new-sock false)
               (>! outch new-sock)
               (recur))
             (log/warn "acceptor closing.."))))

(defrecord Sockman [select bufman this-actor]
  SocketManager
  (connect
    [this out host port]
    (let [socket (doto (SocketChannel/open) (.configureBlocking false))]
      (try
        (.connect socket (InetSocketAddress. (InetAddress/getByName host) port))
        (connector this socket out)
        (catch java.net.ConnectException e
          (log/error "connect: connect to" host ":" port "failed:" (.getMessage e))
          (async/close! out))))
    nil) ; don't accidentally return channel

  (listen
    [this out host port]
    (let [socket (doto (ServerSocketChannel/open) (.configureBlocking false))]
      (.bind (.socket socket) (InetSocketAddress. (InetAddress/getByName host) port))
      (acceptor this socket out))
    nil) ; don't accidentally return channel

  (accept
    [this out socket]
    (go (let [wch (writer this socket)
              rch (reader this socket wch)]
          (>! out [rch wch])))
    nil) ; don't need to wait

  (connect-and-accept
    [this out host port]
    (go (if-let [conn (<! (connect this-actor host port))]
          (accept this-actor out conn)
          (async/close! out)))
    nil) ; deadlock if wait..

  (return-buffer [this out buf] (bufman/return bufman out buf) nil)
  (copy-buffer [this out buf] (bufman/copy bufman out buf) nil))

(defn socket-manager [select bufman]
  (let [actor (SocketManagerActor. (async/chan))]
    (go
     (<! (run-actor actor (Sockman. select bufman actor)))
     (log/warn "Socket manager shutting down")
     (select/shutdown! select)
     (async/close! bufman))
    actor))
