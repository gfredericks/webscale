(ns com.gfredericks.webscale
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [puget.printer :as puget])
  (:import [java.io File]))

(set! *warn-on-reflection* true)

;; The cache file numbered N is the state *before* reading the events
;; in file N.

(defn read-edn-from-file
  [opts file]
  (with-open [r (io/reader file)
              pbr (java.io.PushbackReader. r)]
    (edn/read opts pbr)))

(let [EOF (Object.)]
  (defn ^:private read-state
    "Returns [current-state current-file-num]."
    [reduce-fn init-state file-fn cache-file-fn edn-opts]
    (let [max-file (->> (range)
                        (take-while #(.exists ^File (file-fn %)))
                        (last))
          max-cache-file (when max-file
                           (->> (range max-file -1 -1)
                                (filter #(.exists ^File (cache-file-fn %)))
                                (first)))]
      (if-not max-file
        [init-state 0]
        (let [init-state (if max-cache-file
                           (read-edn-from-file edn-opts (cache-file-fn max-cache-file))
                           init-state)
              files-to-read (range (or max-cache-file 0) (inc max-file))
              edn-opts (assoc edn-opts :eof EOF)

              current-state
              (reduce (fn [state file-num]
                        (with-open [r (io/reader (file-fn file-num))
                                    pbr (java.io.PushbackReader. r)]
                          (loop [ret state]
                            (let [x (edn/read edn-opts pbr)]
                              (if (= EOF x)
                                ret
                                (recur (reduce-fn ret x)))))))
                      init-state
                      files-to-read)]
          [current-state max-file])))))

(def ^:private puget-options
  (assoc puget/*options*
         :strict true
         :print-color false
         :print-fallback true))

(def default-opts
  {:edn-options {}
   :puget-options puget-options
   :prefix "data-"
   :max-file-size 200000
   :cache-state? true})

(defn ^:private pretty-spit
  "Appends the data to the file using the puget pretty-printer."
  [file content puget-options]
  (binding [*out* (io/writer file :append true)]
    (puget/pprint content puget-options)))

(defn create
  "Creates a file-backed stateful-thing.

  Available opts:

  - :prefix        the prefix for filenames (default \"data-\")
  - :max-file-size the size, in bytes, which triggers rolling over
                   to a new file. the files will not be smaller than
                   this amount, but the amount of overage is bounded
                   by the size of the events. Default is 200000.
  - :cache-state?  whether state cache files will be written when
                   rolling over the files. Default is true.
  - :edn-options   a map of options passed to clojure.edn/read, e.g.
                   to provide particular data readers
  - :puget-options a map of options passed to puget, e.g. to print
                   particular data literal tags"
  ([reduce-fn init-state data-dir]
   (create reduce-fn init-state data-dir {}))
  ([reduce-fn init-state data-dir opts]
   (let [opts (merge-with #(if (map? %1)
                             (merge %1 %2)
                             %2)
                          default-opts
                          opts)
         file-fn #(io/file data-dir (str (:prefix opts) % ".edn"))
         cache-file-fn #(file-fn (str % ".cache"))]
     (io/make-parents (file-fn 0))
     (let [[current-state current-file-num]
           (read-state reduce-fn init-state file-fn cache-file-fn (:edn-options opts))]
       (doto (agent current-state)
         (alter-meta! assoc :webscale {:file-fn file-fn
                                       :cache-file-fn cache-file-fn
                                       :current-file-num current-file-num
                                       :reduce-fn reduce-fn
                                       :opts opts}))))))

;; For Super Duper Performance the calling of reduce-fn could be done
;; concurrently with file-writing (in a pipeline way), and the writes
;; could even be batched.
(defn update!
  [ag ev]
  (let [{{:keys [reduce-fn file-fn cache-file-fn current-file-num]
          {:keys [edn-options puget-options max-file-size cache-state?]}
          :opts}
         :webscale}
        (meta ag)
        p (promise)]
    (send-off ag (fn [state]
                   (try
                     ;; presumably there's a race condition here if
                     ;; the file writing gets interrupted. pretty easy
                     ;; to fix by hand though, and impossible not to
                     ;; notice since the agent will have an error, and
                     ;; restarting will have an error trying to read
                     ;; the file.
                     (let [new-state (reduce-fn state ev)
                           ^File
                           current-file (file-fn current-file-num)]
                       (if (and max-file-size
                                (.exists current-file)
                                (<= max-file-size (.length current-file)))
                         (let [next-file-num (inc current-file-num)]
                           (pretty-spit (file-fn next-file-num)
                                        ev puget-options)
                           (when cache-state?
                             (pretty-spit (cache-file-fn next-file-num)
                                          state puget-options))
                           (alter-meta! ag assoc-in [:webscale :current-file-num]
                                        next-file-num))
                         (pretty-spit (file-fn current-file-num)
                                      ev puget-options))
                       (deliver p [new-state])
                       new-state)
                     (catch Throwable t
                       (deliver p t)
                       (throw t)))))
    (await ag)
    (let [x @p]
      (if (instance? Throwable x)
        (throw x)
        (get x 0)))))
