(defproject http-dedup "0.1.0-SNAPSHOT"
  :description "Transparent reverse proxy for deduplicating HTTP requests."
  :url "https://github.com/pclewis/http-dedup"
  :license {:name "CC0"
            :url "http://creativecommons.org/publicdomain/zero/1.0/"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]
                 [org.clojure/tools.cli "0.3.1"]
                 [com.taoensso/timbre "3.1.6"]
                 [jline/jline "2.8"]
                 [org.clojure/tools.macro "0.1.2"]]
  :main ^:skip-aot http-dedup.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
