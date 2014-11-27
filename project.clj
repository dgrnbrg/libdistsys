(defproject distrlib "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/test.check "0.6.1"]
                 [prismatic/schema "0.3.3"]
                 [org.clojure/data.priority-map "0.0.5"]
                 [yesql "0.4.0"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [com.mchange/c3p0 "0.9.2.1"]])
