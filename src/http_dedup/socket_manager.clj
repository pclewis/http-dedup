(ns http-dedup.socket-manager
  (:import [java.nio.channels SocketChannel ServerSocketChannel]
           [java.net InetAddress InetSocketAddress])
  (:require [http-dedup
             [async-utils :as au]
             [select :as select]
             [buffer-manager :as bufman]
             [actor :refer [defactor run-actor]]]
            [clojure.core.async :as async :refer [go go-loop >! <!]]
            [taoensso.timbre :as log]))

(defrecord Connection [read write])

(defactor SocketManager
  "core.async friendly methods for interacting with selector.

   The read-channel will receive a ByteBuffer every time data arrives on the
   socket, and close when the connection closes. The ByteBuffer must be returned
   by writing it to another socket or calling return-buffer.

   ByteBuffers sent to the write-channel will be written to the socket and
   automatically reclaimed.  The connection will be closed when the
   write-channel is closed, after all writes are finished."

  (connect
   "Connect to a given host/port. Emits Connection when finished, or closes on
    error."
   [host port])

  (listen
   "Bind to given host/port and emit accepted Connections."
   [host port])

  (copy-buffer
   [buf]
   "Create a copy of a buffer. Buffers will not be reused until all copies have
    been returned." )

  (return-buffer
   [buf]
   "Return a buffer without writing it to a socket.")

  (return-buffers
   [coll-or-ch]
   "Return all buffers from a collection or channel."))

(defmacro safe-io
  "Log and return -1 if body throws an IOException."
  [& body]
  `(try ~@body
       (catch java.io.IOException e#
         (log/warnf "%s failed: %s: %s"
                    ~(str body) (.getName (.getClass e#)) (.getMessage e#))
         -1)))

(defn- writer [{:keys [bufman select]} socket]
  (let [inch (async/chan 64)]
    (go
     (try
       (loop []
         (when-let [buf (<! inch)]
           (log/trace socket "received a buffer to write" buf)
           (while (.hasRemaining buf)
             (when-not (and (<! (select/write select socket))
                            (< 0 (safe-io (.write socket buf))))
               (log/debug "write: socket is closed, discarding buffer:" socket)
               (.limit buf 0)))
           (bufman/return bufman buf)
           (recur)))
       (finally
         (log/debug "write: closing connection" socket)
         (select/close select socket))))
    inch))

(defn- reader [{:keys [bufman select]} socket wch]
  (let [outch (async/chan 64)]
    (go
     (try
       (loop []
         (if (<! (select/read select socket))
           (let [buf (<! (bufman/request bufman))
                 n-read (safe-io (.read socket buf))]
             (log/trace "Received" n-read "bytes on socket" buf)
             (if (> 0 n-read)
               (bufman/return bufman buf)
               (do (.flip buf)
                   (or (>! outch buf)
                       (bufman/return bufman buf))
                   (recur))))))
       (finally
         (log/debug "read: connection closed")
         (async/close! outch)
         (async/close! wch)
         (select/close select socket))))
    outch))

(defn- send-channels [{:keys [select] :as this} out socket]
  (let [wch (writer this socket)
        rch (reader this socket wch)]
    (if (async/put! out (Connection. rch wch))
      true
      (do (log/error "send-channels: nobody received created channels,"
                     "destroying them and closing socket!")
          (async/close! wch)
          (select/close select socket)
          false))))

(defn- connector [{:keys [select] :as this} socket outch]
  (go
   (try
     (loop []
       (when (<! (select/connect select socket))
         (let [result (safe-io (.finishConnect socket))]
           (case result
             true (send-channels this outch socket)
             -1 nil
             (recur)))))
     (finally
       (async/close! outch)))))

(defn- acceptor [{:keys [select] :as this} socket outch]
  (go
   (try
     (loop []
       (when (<! (select/accept select socket))
         (let [new-sock (safe-io (.accept socket))]
           (when-not (= -1 new-sock)
             (.configureBlocking new-sock false)
             (if (send-channels this outch new-sock)
               (recur)
               (log/error "acceptor: channel closed, no longer accepting"
                          "connections."))))))
     (finally
       (select/close select socket)
       (async/close! outch)))))

(defrecord Sockman [select bufman this-actor]
  SocketManager
  (connect
    [this out host port]
    (let [socket (doto (SocketChannel/open) (.configureBlocking false))]
      (try
        (.connect socket (InetSocketAddress. (InetAddress/getByName host) port))
        (connector this socket out)
        (catch java.net.ConnectException e
          (log/errorf "connect: connect to %s:%d failed: %s"
                      host port (.getMessage e))
          (async/close! out))))
    nil) ; don't accidentally return channel

  (listen
    [this out host port]
    (let [socket (doto (ServerSocketChannel/open) (.configureBlocking false))]
      (.bind (.socket socket)
             (InetSocketAddress. (InetAddress/getByName host) port))
      (acceptor this socket out))
    nil) ; don't accidentally return channel

  (return-buffers
    [this out coll-or-ch]
    (cond
     (coll? coll-or-ch) (doall (map #(bufman/return bufman out %) coll-or-ch))
     (au/readable? coll-or-ch) (async/pipe coll-or-ch
                                           (async/map> #(vector :return out %)
                                                       bufman)
                                           false))
    nil)

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
