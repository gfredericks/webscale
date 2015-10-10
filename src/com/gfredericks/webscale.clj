(ns com.gfredericks.webscale
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [com.gfredericks.puget.printer :as puget]))

;;
;; TODO:
;; - feature where it rolls over to new files with the current state
;;   cached at the top (or in a different file)
;;

(defn ^:private reduce-file
  [reduce-fn init-state file edn-opts]
  (if (.exists file)
    (let [o (Object.)
          edn-opts (assoc edn-opts :eof o)]
      (with-open [r (java.io.PushbackReader. (io/reader file))]
        (loop [ret init-state]
          (let [x (edn/read edn-opts r)]
            (if (= o x)
              ret
              (recur (reduce-fn ret x)))))))
    init-state))

(def ^:private puget-options
  (assoc puget/*options*
         :strict true
         :print-color false
         :print-fallback true))

(def default-opts
  {:edn-options {}
   :puget-options puget-options
   :prefix "data-"})

(defn ^:private pretty-spit
  "Appends the data to the file using the puget pretty-printer."
  [file content puget-options]
  (binding [*out* (io/writer file :append true)]
    (puget/pprint content puget-options)))

(defn create
  "Creates a file-backed stateful-thing."
  ([reduce-fn init-state data-dir]
   (create reduce-fn init-state data-dir {}))
  ([reduce-fn init-state data-dir opts]
   (let [opts (merge-with #(if (map? %1)
                             (merge %1 %2)
                             %2)
                          default-opts
                          opts)
         file (io/file data-dir (str (:prefix opts) "0.edn"))]
     (io/make-parents file)
     (doto (agent (reduce-file reduce-fn init-state file (:edn-options opts)))
       (alter-meta! assoc ::file file ::f reduce-fn ::opts opts)))))

(defn update!
  [ag ev]
  (let [{:keys [::f ::file ::opts]} (meta ag)
        p (promise)]
    (send-off ag (fn [state]
                   (try
                     ;; presumably there's a race condition here if
                     ;; the file writing gets interrupted. pretty easy
                     ;; to fix by hand though, and impossible not to
                     ;; notice since the agent will have an error, and
                     ;; restarting will have an error trying to read
                     ;; the file.
                     (let [new-state (f state ev)]
                       (pretty-spit file ev (:puget-options opts))
                       (deliver p [new-state])
                       new-state)
                     (catch Throwable t
                       (deliver p t)
                       (throw t)))))
    (let [x @p]
      (if (instance? Throwable x)
        (throw x)
        (get x 0)))))
