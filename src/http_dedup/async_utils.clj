(ns http-dedup.async-utils
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.pprint :refer [pprint]]
            [taoensso.timbre :as log]))

(defmacro defasync
  "Convert a form like:

   (defasync name [field1 field2]
       (create [constructor args] (impl))?
       (destroy (impl))?
       (fn private-fn [args] (impl))+
       (public-fn [args] (impl))+)

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
   that are declared before themselves.

   The specified fields are contained in a map initialized by the constructor.
   If any public fn returns a map, the state will be updated by merging it.
   If there is a field called 'this', it will be initialized to the channel
   returned by the constructor, and can be used to call public-fns."
  [name fields & fns]
  (let [[[ctor] fns] (split-with #(= 'create (first %)) fns)
        [[dtor] fns] (split-with #(= 'destroy (first %)) fns)
        [local-fns methods] (split-with #(= 'fn (first %)) fns)
        msg_ (gensym "msg")
        state_ (gensym "state")
        chan_ (if (some #{'this} fields)
                'this
                (gensym "ch"))
        fields (remove #{'this} fields)]
    `(do
       ~@(for [f methods :let [fname (first f)
                               args (second f)]]
           `(defn ~fname
              ~@(if (= 'out (first args))
                  `((~(into [name] (rest args))
                     (let [~(first args) (async/chan)]
                       (~fname ~name ~@args)
                       ~(first args)))
                    (~(into [name] args)
                     (or (async/put! ~name ~(into [(keyword fname)] args))
                         (async/close! ~'out))))
                  `(~(into [name] args)
                    (async/put! ~name ~(into [(keyword fname)] args))))))

       (defn ~name ~(if ctor (second ctor) `[])
         (let [~chan_ (async/chan)]
           (go-loop [{:keys ~fields :as ~state_}
                     ~@(if ctor (drop 2 ctor) `{})]
                    (let [~@(apply concat (for [f local-fns]
                                            (list (second f) f)))]
                      (if-let [~msg_ (<! ~chan_)]
                        (do
                          (log/trace ~(str name) "received message:" ~msg_)
                          (recur
                           (let [res# (try
                                    (condp = (first ~msg_)
                                      :debug-state (log/debug
                                                    (let [w# (java.io.StringWriter.)]
                                                      (pprint ~state_ w#)
                                                      (.toString w#)))
                                      ~@(apply concat (for [f methods]
                                                        (list (keyword (first f))
                                                              `(let [~(second f) (rest ~msg_)]
                                                                 ~@(drop 2 f))))))
                                    (catch Throwable t
                                      (log/error t "Exception in message handler:" ~msg_)))]
                             (if (map? res#)
                               (merge ~state_ res#)
                               ~state_))))
                        (do (log/trace ~(str name) "closed.")
                            ~@(when dtor (rest dtor))))))
           ~chan_)))))


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
     (if (fn? ch)
       (let [nch (async/chan)]
         (apply drain nch ch f args)
         nch)
       (let [fa (apply partial f args)]
         (go-loop-<! ch msg (fa msg))))))

(defmacro thread [& body]
  `(async/thread
    (try
      ~@body
      (catch Throwable t#
        (log/error t# "Thread exiting with exception")))))
