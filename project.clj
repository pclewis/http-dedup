(defproject http-dedup "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Proprietary"
            :url "http://dctf.ytmnd.com"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]
                 [org.clojure/tools.cli "0.3.1"]
                 [com.taoensso/timbre "3.1.6"]
                 [jline/jline "2.8"]]
  :main ^:skip-aot http-dedup.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
