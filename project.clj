(defproject com.gfredericks/webscale "0.1.1-SNAPSHOT"
  :description "Casual file-based persistence in Clojure."
  :url "https://github.com/gfredericks/webscale"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.gfredericks.forks.mvxcvi/puget "0.8.1-f7bb55e"]
                 ;; [mvxcvi/puget "0.8.1"]
                 ]
  :deploy-repositories [["releases" :clojars]]
  :profiles {:dev {:dependencies [[me.raynes/fs "1.4.6"]
                                  [org.clojure/test.check "0.8.2"]]}})
