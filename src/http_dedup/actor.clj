(ns http-dedup.actor
  "core.async-based actor model!"
  (:require [clojure.core.async :as async :refer [>! <! >!! <!! go go-loop]]
            [clojure.core.async.impl.protocols :as async-protos]
            [clojure.tools.macro :refer [name-with-attributes]]
            [http-dedup.async-utils :as au]
            [taoensso.timbre :as log]))

(defprotocol Actor
  (dispatch-message [this that msg args]))

(defn send-msg [ch msg out & args]
  (let [out (or out (async/chan))]
    (or (async/put! ch (into [msg out] args))
        (async/close! out))
    out))

(defn- make-spec [leading-args body [name & rest]]
    (let [[name [args & forms]] (name-with-attributes name rest)]
      (cons name
            (cons (into leading-args args)
                  (when body (body name args))))))

(defn- make-sender [farg name args]
  (list (concat `(send-msg ~'mailbox ~(keyword name) ~farg) args)))

(defn- make-proto-body [name args]
  (cons (into '[this out] args) (when-let [d (:doc (meta name))]
                                  (cons d nil))))

(defn- make-dispatch [[name & _]]
  `(~(keyword name)
     (apply ~name ~'that ~'args)))

(defmacro defactor [name & rest]
  (let [[name specs] (name-with-attributes name rest)]
    `(do
       (defprotocol ~name
         ~@(map #(make-spec '[this] make-proto-body %) specs))

       (defrecord ~(symbol (str name "Actor")) [~'mailbox]
         ~name
         ~@(map #(make-spec '[this] (partial make-sender nil) %) specs)
         ~@(map #(make-spec '[this out] (partial make-sender 'out) %) specs)
         Actor
         (dispatch-message ~'[this that method args]
           (case ~'method
             ~@(mapcat make-dispatch specs)))
         async-protos/Channel
         (close! [_] (async-protos/close! ~'mailbox))
         async-protos/WritePort
         (put! ~'[_ val fn1] (async-protos/put! ~'mailbox ~'val ~'fn1))))))

(defn run-actor [actor inst]
  (let [mailbox (:mailbox actor)]
    (go-loop [state inst]
      (when-let [[method & args :as msg] (<! mailbox)]
        (log/trace actor "got message:" msg)
        (let [result (try (dispatch-message actor state method args)
                          (catch Throwable t
                            (log/error t "error handling message" {:msg msg :actor actor :state state})
                            {}))
              result (if (au/readable? result)
                       (<! result)
                       result)]
          (recur (if (map? result)
                   (merge state result)
                   state)))))))
