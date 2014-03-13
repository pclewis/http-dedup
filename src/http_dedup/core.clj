(ns http-dedup.core
  (:gen-class)
  (:import [java.net InetAddress InetSocketAddress Socket StandardSocketOptions]
           [java.nio ByteBuffer]
           [java.nio.channels ServerSocketChannel SocketChannel Selector SelectionKey]
           [java.nio.charset StandardCharsets]) )

(defmulti handle-event #(.readyOps %))

(defmethod handle-event (SelectionKey/OP_ACCEPT) [sk]
  (let [new-channel (doto (.. sk channel accept)
                      (.configureBlocking false))
        new-sk (.register new-channel (.selector sk) SelectionKey/OP_READ)]
    (.attach new-sk {:read-buffer (ByteBuffer/allocate 8192)
                     :write-buffer (ByteBuffer/allocate 8192)
                     :request (atom "")
                     :state (atom :init)})
    (println "Accepted connection from " (.getRemoteAddress new-channel))))

(defmethod handle-event (SelectionKey/OP_WRITE) [sk]
  (let [wb]))

(defmethod handle-event (SelectionKey/OP_READ) [sk]
  (let [buffer (:read-buffer (.attachment sk))
        start  (.limit buffer)
        channel (.channel sk)
        n-read (.read channel buffer)]
    (if (< n-read 0)
      (do
        (println "Read error from " (.getRemoteAddress channel))
        (.cancel sk)
        (.close channel))
      (do
        (let [new-str (String. (.array buffer) StandardCharsets/US_ASCII)])))))

(defn make-server [listen-port]
  (let [selector (Selector/open)
        socket (doto (ServerSocketChannel/open)
                   (.configureBlocking false)
                   (.setOption StandardSocketOptions/SO_REUSEADDR true)
                   (.bind (InetSocketAddress. (InetAddress/getLocalHost) listen-port)))]
    (.register socket selector (SelectionKey/OP_ACCEPT))
    selector))

(defmacro thread [& body]
  `(.start
    (Thread. (fn [] (try ~@body
                        (catch Throwable t#
                          (println "Thread died with exception: ")
                          (clojure.stacktrace/print-cause-trace t#))
                        (finally (println "Thread exiting")))))))

(defn main-loop [selector handler]
  (let [breaker (atom true)]
    (thread
      (while @breaker
        (when (< 0 (.select selector))
          (let [keys (.selectedKeys selector)]
            (doseq [k keys] (handler k))
            (.clear keys)))))
    breaker))

(defn shutdown [selector breaker]
  (reset! breaker nil)
  (doseq [k (.keys selector)]
    (.cancel k)
    (.close (.channel k)))
  (.wakeup selector)
  (.close selector))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [[listen-port connect-port] (map #(Integer. %) args)]
    (if (and (< 0 listen-port 65536)
             (< 0 connect-port 65536))
      (start-server listen-port)
      (println "Usage: http-dedup [listen-port] [connect-port]"))))

(comment
  (def server (make-server 8081))

  (def breaker
    (main-loop server handle-event))

  (shutdown server breaker)



  )
