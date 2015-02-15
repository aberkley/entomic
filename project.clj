(defproject org.clojars.aberkley/entomic "2.1.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.datomic/datomic-free "0.9.4880.2" :scope "test"]
                 [org.clojars.aberkley/utilities "0.1.3-SNAPSHOT"]
                 [clj-time "0.6.0"]
                 [org.clojure/core.incubator "0.1.3"]
                 [com.stuartsierra/component "0.2.2"]]
  :offline? false
  :jvm-opts ["-Xmx768M"]
  :aot :all)
