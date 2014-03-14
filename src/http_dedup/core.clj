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

(defn pre-swap!
  "swap! but return the value that was swapped out instead of the value that was swapped in."
  [atom f & args]
  (loop [old-value @atom]
    (if (compare-and-set! atom old-value (apply f old-value args))
      old-value
      (recur @atom))))

(declare new-server-connection)
(defn make-connection [selector port write-buffer ifr flight-key]
  (let [conn (new-server-connection write-buffer ifr flight-key)
        channel (doto (SocketChannel/open)
                  (.configureBlocking false)
                  (.connect (InetSocketAddress. (InetAddress/getByName nil) port)))
        sk (.register channel selector SelectionKey/OP_CONNECT)
        nc (assoc conn :sk sk)]
    (.attach sk nc)
    nc))

(defn update-buffer [buffer new-str]
  (.clear buffer)
  (.put buffer (.getBytes new-str ASCII)))

(defn join-flight [rqs flight-key connection]
  (get (swap! rqs (partial merge-with concat) {flight-key [connection]})
       flight-key))

(defn launch-flight [rqs flight-key]
  (get (pre-swap! rqs dissoc flight-key)
       flight-key))

(declare end-flight)
(declare flight-loop-handler)
(defn client-read-handler [sk connection]
  (let [read-buffer (:read-buffer connection)
        current-str (.toString (.decode (:decoder connection) (.flip (.duplicate read-buffer))))
        terminator (.indexOf current-str "\r\n\r\n")
        request (when (<= 0 terminator) (subs current-str 0 terminator))]
    (when request
      (let [flight-key [(-> sk .channel .socket .getRemoteSocketAddress .getAddress) request]
            flight (join-flight (:in-flight-requests connection) flight-key connection)]
        (if (= 1 (count flight))
          (do
            (println "Starting a flight " (first (clojure.string/split-lines request)))
            (if (<= 0 (.indexOf request "\r\nConnection"))
              (update-buffer read-buffer (clojure.string/replace-first
                                          current-str
                                          #"(?m)^Connection: (.*)$" "Connection: close")))
            (let [c (make-connection (.selector sk)
                                     (:connect-port connection)
                                     (.flip (.duplicate (:read-buffer connection)))
                                     (:in-flight-requests connection)
                                     flight-key)]
              (.attach sk (assoc connection
                            :on-loop flight-loop-handler
                            :on-read nil
                            :on-close nil
                            :passengers [c]))))
          (println "Joined a flight!"))))))

(defn shared-compact [root others]
  (let [n-compactable (apply min (map #(.position %) others))
        old-pos (.position root)]
    (println "Trying to compact, n-compactable =" n-compactable)
    (println "Root is at" old-pos "/" (.limit root))
    (when (< 0 n-compactable)
      (.position root n-compactable)
      (.compact root)
      (.position root (- old-pos n-compactable))
      (doseq [buf others]
        (.position buf (- (.position buf) n-compactable)))))
  (let [new-limit (.position root)]
    (doseq [buf others]
      (.limit buf new-limit))))

(defn flight-loop-handler [sk connection]
  (let [passengers (:passengers connection)
        read-buffer (:read-buffer connection)]
    (println "Flight loop" (count passengers) "passengers")
    (shared-compact read-buffer (map :write-buffer passengers))))

(defn server-read-handler [sk connection]
  (let [passengers (launch-flight (:in-flight-requests connection)
                                  (:flight-key connection))]
    (when (not-empty passengers)
      (println "Launching flight with" (count passengers) "passengers, flight key:")
      (println (:flight-key connection))
      (let [passengers (map #(assoc % :write-buffer
                                    (.flip (.duplicate (:read-buffer connection))))
                            passengers)
            new-connection (assoc connection
                             :on-read nil
                             :on-loop flight-loop-handler
                             :on-close end-flight
                             :passengers passengers)]
        (.attach sk new-connection)
        (doseq [p passengers] (.attach (:sk p) p))))))

(defn close-connection [connection]
  (let [sk (:sk connection)
        channel (.channel sk)]
    (println "Connection closed " (.getRemoteSocketAddress (.socket channel)))
    (when-let [on-close (:on-close connection)]
      (on-close sk connection))
    (.cancel sk)
    (.close channel)))

(defn end-flight [sk connection]
  (doseq [p (:passengers connection)
          :let [wb (:write-buffer p)]]
    (if (= (.position wb) (.limit wb))
      (close-connection p)
      (reset! (:want-close p) true))))

(defn new-client-connection [sk]
  {:read-buffer (ByteBuffer/allocate 8192)
   :decoder (.newDecoder ASCII)
   :want-close (atom false)
   :linked-connections (atom [])
   :sk sk
   :on-read client-read-handler
   :on-close nil})

(defn new-server-connection [wb ifr fk]
  {:read-buffer (ByteBuffer/allocate 8192)
   :write-buffer wb
   :want-close (atom false)
   :in-flight-requests ifr
   :flight-key fk
   :on-read server-read-handler
   :on-close nil})

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
                      (.configureBlocking false))
        new-sk (.register new-channel (.selector sk) SelectionKey/OP_READ)]
    (.attach new-sk (merge (.attachment sk) (new-client-connection new-sk)))
    (println "Accepted connection from " (.getRemoteSocketAddress (.socket new-channel)))))

(defmethod handle-event (SelectionKey/OP_WRITE) [sk]
  (let [connection (.attachment sk)
        buffer (:write-buffer connection)]
    ;(println "Writing.." (.toString (.decode (.newDecoder ASCII) (.duplicate buffer))))
    (.write (.channel sk) buffer)
    (when (and (= (.limit buffer) (.position buffer))
               (deref (:want-close connection)))
      (close-connection connection))))

(defmethod handle-event (SelectionKey/OP_READ) [sk]
  (let [attachment (.attachment sk)
        buffer (:read-buffer attachment)
        start  (.limit buffer)
        channel (.channel sk)
        n-read (try (.read channel buffer)
                    (catch java.io.IOException e -1))]
    (if (< n-read 0)
     (close-connection attachment)
      (when-let [on-read (:on-read attachment)]
        (on-read sk attachment)))))

(defn make-server [listen-port connect-port]
  (let [selector (Selector/open)
        socket (ServerSocketChannel/open)]
    (try
      (.configureBlocking socket false)
      (.bind (.socket socket) (InetSocketAddress. (InetAddress/getByName "0.0.0.0") listen-port))
      (.register socket selector (SelectionKey/OP_ACCEPT) {:connect-port connect-port
                                                           :in-flight-requests (atom {})})
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
           (when-let [lh (:on-loop (.attachment k))]
             (lh k (.attachment k))))
          (doseq [k (.keys selector)]
            (when (.isValid k)
              (when-let [buffer (:write-buffer (.attachment k))]
                (if (< (.position buffer) (.limit buffer))
                  (.interestOps k (bit-or (.interestOps k) SelectionKey/OP_WRITE))
                  (.interestOps k (bit-and-not (.interestOps k) SelectionKey/OP_WRITE)) ))
              (when-let [buffer (:read-buffer (.attachment k))]
                (if (< (.position buffer) (.limit buffer))
                  (.interestOps k (bit-or (.interestOps k) SelectionKey/OP_READ))
                  (.interestOps k (bit-and-not (.interestOps k) SelectionKey/OP_READ)) )))))))
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
  (def server (make-server 8081 3000))

  (def breaker
    (main-loop server handle-event))

  (shutdown server breaker)

  )
