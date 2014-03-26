(ns http-dedup.util
  (:import [java.nio.charset Charset CharsetDecoder]))

(def ASCII (Charset/forName "US-ASCII"))

(defn test-bits
  "Returns true if all bits in y are set in x."
  [x y] (= y (bit-and x y)))

(defn bit-seq
  "Return lazy seq of bit values (powers of 2) that are set in n (default -1, i.e. all bits set)."
  ([] (take-while (complement zero?) (iterate #(bit-shift-left % 1) 1)))
  ([n] (filter #(test-bits n %) (take-while #(or (< n 0) (<= % n)) (bit-seq)))))

(defn pre-swap!
  "swap! but return the value that was swapped out instead of the value that was swapped in."
  [atom f & args]
  (loop [old-value @atom]
    (if (compare-and-set! atom old-value (apply f old-value args))
      old-value
      (recur @atom))))

(defn bytebuf-to-str [bb] (.toString (.decode ASCII (.duplicate bb))))

(defn str-to-bytebuf [s] (.encode ASCII s))

(defn drop-bytes!
  "Modify a sequence of buffers so that the first n bytes are removed.
   If n is bigger than the first buffer, it will have .remaining=0, and so on."
  [n bufs]
  (when-let [[buf & rest] (seq bufs)]
    (let [size (.remaining buf)]
      (if (> n size)
        (do (.position buf (.limit buf))
            (recur (- n size) rest))
        (.position buf (+ (.position buf) n))))))
