(ns http-dedup.core
  (:gen-class)
  (:import [java.net InetAddress InetSocketAddress Socket]
           [java.nio ByteBuffer]
           [java.nio.channels ServerSocketChannel SocketChannel Selector SelectionKey]
           [java.nio.charset Charset CharsetDecoder]))

(def ASCII (Charset/forName "US-ASCII"))

(defn bit-seq
  ([] (iterate #(bit-shift-left % 1) 1))
  ([n] (keep #(when (= % (bit-and n %)) %) (take-while #(<= % n) (bit-seq)))))

(declare new-server-connection)
(defn make-connection [selector port read-buffer write-buffer]
  (doto (SocketChannel/open)
    (.configureBlocking false)
    (.connect (InetSocketAddress. (InetAddress/getByName nil) port))
    (.register selector SelectionKey/OP_CONNECT (new-server-connection read-buffer write-buffer))))

(defn update-buffer [buffer new-str]
  (.clear buffer)
  (.put buffer (.getBytes new-str ASCII)))

(defn client-read-handler [sk connection]
  (let [read-buffer (:read-buffer connection)
        new-str (.toString (.decode (:decoder connection) (.flip (.duplicate read-buffer))))
        terminator (.indexOf new-str "\r\n\r\n")]
    (when (<= 0 terminator)
      (println "Saw a full request!" new-str)
      (if (<= 0 (.indexOf new-str "\r\nConnection"))
        (update-buffer read-buffer (clojure.string/replace-first new-str
                                                                 #"(?m)^Connection: (.*)$"
                                                                 "Connection: close")))
      (make-connection (.selector sk) (:connect-port connection) (:write-buffer connection) read-buffer))) )

(defn close-linked-connections [sk connection]
  (doseq [lc (:linked-connections connection)]
    (reset! (:want-close lc) true)))

(defn new-connection []
  {:read-buffer (ByteBuffer/allocate 8192)
   :write-buffer (ByteBuffer/allocate 8192)
   :decoder (.newDecoder ASCII)
   :want-close (atom false)
   :linked-connections (atom [])
   :on-read client-read-handler
   :on-close close-linked-connections})

(defn new-server-connection [rb wb]
  {:read-buffer rb
   :write-buffer wb
   :want-close (atom false)
   :on-close close-linked-connections})

(defmulti handle-event #(.readyOps %))

(defmethod handle-event :default [sk]
  (doseq [b (bit-seq (.readyOps sk))]
    ((get-method handle-event b) sk)))

(defmethod handle-event (SelectionKey/OP_CONNECT) [sk]
  (when (.finishConnect (.channel sk))
    (println "Connection established")
    (.interestOps sk SelectionKey/OP_READ)))

(defmethod handle-event (SelectionKey/OP_ACCEPT) [sk]
  (let [new-channel (doto (.. sk channel accept)
                      (.configureBlocking false)
                      (.register (.selector sk) SelectionKey/OP_READ
                                 (merge (.attachment sk) (new-connection))))     ]
    (println "Accepted connection from " (.getRemoteSocketAddress (.socket new-channel)))))

(defmethod handle-event (SelectionKey/OP_WRITE) [sk]
  (let [buffer (:write-buffer (.attachment sk))]
    (.flip buffer)
    (.write (.channel sk) buffer)
    (.compact buffer)
    (if (>= 0 (.position buffer))
      (.interestOps sk (bit-and-not (.interestOps sk) SelectionKey/OP_WRITE)))))

(defmethod handle-event (SelectionKey/OP_READ) [sk]
  (let [attachment (.attachment sk)
        buffer (:read-buffer attachment)
        start  (.limit buffer)
        channel (.channel sk)
        n-read (.read channel buffer)]
    (if (< n-read 0)
      (do
        (println "Read error from " (.getRemoteSocketAddress (.socket channel)))
        (.cancel sk)
        (.close channel))
      (when-let [on-read (:on-read attachment)]
        (on-read sk attachment)))))

(defn make-server [listen-port connect-port]
  (let [selector (Selector/open)
        socket (ServerSocketChannel/open)]
    (try
      (.configureBlocking socket false)
      (.bind (.socket socket) (InetSocketAddress. (InetAddress/getByName nil) listen-port))
      (.register socket selector (SelectionKey/OP_ACCEPT) {:connect-port connect-port})
      selector
      (catch Throwable t
        (.close socket)
        (.close selector)
        (throw t)))))

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
            (.clear keys))
          (doseq [k (.keys selector)]
            (when-let [buffer (:write-buffer (.attachment k))]
              (when (< 0 (.position buffer))
                (.interestOps k (bit-or (.interestOps k) SelectionKey/OP_WRITE))))))))
    breaker))

(defn shutdown [selector breaker]
  (reset! breaker nil)
  (doseq [k (.keys selector)]
    (.cancel k)
    (.close (.channel k)))
  (.wakeup selector)
  (.close selector))

(comment defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [[listen-port connect-port] (map #(Integer. %) args)]
    (if (and (< 0 listen-port 65536)
             (< 0 connect-port 65536))
      (start-server listen-port)
      (println "Usage: http-dedup [listen-port] [connect-port]"))))

(comment
  (def server (make-server 8081 8080))

  (def breaker
    (main-loop server handle-event))

  (shutdown server breaker)




  (reset! breaker nil)

  (shutdown server (atom nil))


  (def bb (ByteBuffer/allocate 1024))


  (.clear bb)

  (.put bb (.getBytes "Hello!\r\nConnection: keep-alive\r\nStuff!\r\n\r\nMore stuff!" ASCII))
  (update-buffer bb (clojure.string/replace-first
                     (.toString (.decode (.newDecoder ASCII) (.flip (.duplicate bb))))
                     #"(?m)^Connection: (.*)$"
                     "Connection: close"))

  (remove-connection-header bb


                            )



  )
