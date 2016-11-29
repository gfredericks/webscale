(defproject com.gfredericks/webscale "0.1.3"
  :description "Casual file-based persistence in Clojure."
  :url "https://github.com/gfredericks/webscale"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [mvxcvi/puget "1.0.0"]]
  :deploy-repositories [["releases" :clojars]]
  :profiles {:dev {:dependencies [[me.raynes/fs "1.4.6"]
                                  [org.clojure/test.check "0.9.0"]
                                  [bond "0.2.5"]]}})
