(defproject fallout/jepsen "0.0.8-SNAPSHOT"
  :description "Fallout fork of Jepsen"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.fressian "0.2.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-time "0.6.0"]
                 [knossos "0.3.1"]
                 [clj-ssh "0.5.14"]
                 [gnuplot "0.1.0"]
                 [hiccup "1.0.5"]
                 [org.clojars.achim/multiset "0.1.0"]
                 [fipp "0.6.2"]
                 [byte-streams "0.1.4"]]
  :omit-source true
  :aot [knossos.core #"jepsen\..*"]
  :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods" "-server"])
