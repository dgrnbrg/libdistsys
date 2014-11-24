(defproject distrlib "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opt ["-XX:-OmitStackTraceInFastThrow"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/test.check "0.6.1"]
                 [org.clojure/data.priority-map "0.0.5"]
                 [potemkin "0.3.11"]])
