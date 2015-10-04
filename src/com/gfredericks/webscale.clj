(ns com.gfredericks.webscale
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [puget.printer :as puget]))

;;
;; TODO:
;; - feature where it rolls over to new files with the current state
;;   cached at the top (or in a different file)
;; - modify puget to be locally extensible
;;   - https://github.com/greglook/puget/issues/23
;;

(defn ^:private reduce-file
  [reduce-fn init-state file]
  (if (.exists file)
    (let [o (Object.)]
      (with-open [r (java.io.PushbackReader. (io/reader file))]
        (loop [ret init-state]
          (let [x (edn/read {:eof o} r)]
            (if (= o x)
              ret
              (recur (reduce-fn ret x)))))))
    init-state))

(def ^:private puget-options
  (assoc puget/*options*
         :strict true
         :print-color false
         :print-fallback true))

(defn ^:private pretty-spit
  "Appends the data to the file using the puget pretty-printer."
  [file content]
  (binding [*out* (io/writer file :append true)]
    (puget/pprint content puget-options)))

(defn create
  "Creates a file-backed stateful-thing."
  ([reduce-fn init-state data-dir]
   (create reduce-fn init-state data-dir {}))
  ([reduce-fn init-state data-dir
    {:keys [prefix]
     :or {prefix "data-"}}]
   (let [file (io/file data-dir (str prefix "0.edn"))]
     (io/make-parents file)
     (doto (agent (reduce-file reduce-fn init-state file))
       (alter-meta! assoc ::file file ::f reduce-fn)))))

(defn update!
  [ag ev]
  (let [{:keys [::f ::file]} (meta ag)
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
                       (pretty-spit file ev)
                       (deliver p [new-state])
                       new-state)
                     (catch Throwable t
                       (deliver p t)
                       (throw t)))))
    (let [x @p]
      (if (instance? Throwable x)
        (throw x)
        (get x 0)))))
