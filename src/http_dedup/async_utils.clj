(ns http-dedup.async-utils
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]))

(defmacro defasync
  "Convert a form like:

   (defasync name [field1 field2] ([constructor args] constructor)
       (fn private-fn [args] (impl))
       (public-fn [args] (impl)))

   Into a function that returns a channel on which names of and arguments
   to public functions can be written to invoke them.

   Ex: (let [ch (name [arg1 arg2])]
         (>!! ch [:public-fn arg]))

   Functions will also be generated for each public-fn that take a
   channel and arguments and write the correct message to the channel.
   If the first argument to a public-fn is called 'out', the generated
   function will automatically create and return it.

   Note private-fns are declared in order because core.async does not
   properly support letfn. i.e., private fns can only call other private-fns
   that are declared before themselves."
  [name fields ctor & fns]
  (let [{local-fns true, methods false} (group-by #(= (first %) 'fn) fns)
        msg_ (gensym "msg")
        state_ (gensym "state")]
    `(do
       (defn ~name ~(first ctor)
         (let [ch# (async/chan)]
           (go-loop [{:keys ~fields :as ~state_} (merge {:this ch#} ~@(rest ctor))]
                    (let [~@(apply concat (for [f local-fns]
                                            (list (second f) f)))]
                      (if-let [~msg_ (<! ch#)]
                        (do (println "Got message: " ~msg_)
                          (recur
                           (merge ~state_
                                  (try
                                    (condp = (first ~msg_)
                                      ~@(apply concat (for [f methods]
                                                        (list (keyword (first f))
                                                              `(let [~(second f) (rest ~msg_)]
                                                                 ~@(drop 2 f))))))
                                    (catch Throwable t
                                      (println "Exception in message handler:")
                                      (clojure.stacktrace/print-cause-trace t)
                                      {}) ))))

                        (println "Loop dying"))))
           ch#))
       ~@(apply concat (for [f methods :let [fname (first f)
                                             fname! (symbol (str fname "!"))
                                             fname!! (symbol (str fname "!!"))
                                             args (second f)]]
                         (list
                          (if (= 'out (first args))
                            `(defmacro ~fname! ~(into [name] (rest args))
                               (let [~(first args) (async/chan)]
                                 (>! ~name ~(into [(keyword fname)] args))
                                 ~(first args)))
                            `(defmacro ~fname! ~(into [name] args)
                               (>! ~name ~(into [(keyword fname)] args))))

                          (if (= 'out (first args))
                            `(defn ~fname!! ~(into [name] (rest args))
                               (let [~(first args) (async/chan)]
                                 (async/>!! ~name ~(into [(keyword fname)] args))
                                 ~(first args)))
                            `(defn ~fname!! ~(into [name] args)
                               (async/>!! ~name ~(into [(keyword fname)] args))))

                          (if (= 'out (first args))
                            `(defn ~fname ~(into [name] (rest args))
                               (let [~(first args) (async/chan)]
                                 (go (>! ~name ~(into [(keyword fname)] args)))
                                 ~(first args)))
                            `(defn ~fname ~(into [name] args)
                               (go (>! ~name ~(into [(keyword fname)] args)))))))))))


;(remove-ns 'http-dedup.async-utils)

(defmacro peek!
  ([chan] `(peek! ~chan 0))
  ([chan timeout]
     `(async/alt! ~chan ([v#] v#)
                  ~(if (= timeout 0) `:default `(async/timeout ~timeout)) nil)))

(defn peek!!
  ([chan] (peek!! chan 0))
  ([chan timeout]
     (async/alt!! chan ([v] v) (async/timeout timeout) nil)))


(defn get!!
  ([chan] (get!! chan false 0))
  ([chan none] (get!! chan none 0))
  ([chan none timeout]
     (if (= 0 timeout)
       (async/alt!! chan ([v] v) :default none)
       (async/alt!! chan ([v] v) (async/timeout timeout) none))))

(defmacro go-loop-<!
  "A loop that will take items from ch and bind them to binding.
   Will loop until the channel is closed."
  [ch binding & body]
  `(go-loop []
            (when-let [~binding (<! ~ch)]
              ~@body
              (recur))))

(defn drain
  ([ch] (drain ch identity))
  ([ch f & args]
     (let [fa (apply partial f args)]
       (go-loop-<! ch msg (fa msg)))))
