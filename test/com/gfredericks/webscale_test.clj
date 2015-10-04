(ns com.gfredericks.webscale-test
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.webscale :as webscale]
            [me.raynes.fs :as fs]))

(def gen-key (gen/elements (->> (range (int \a) (inc (int \z)))
                                (map char)
                                (map str)
                                (map keyword))))

(def gen-event
  (gen/hash-map :key gen-key
                :incr gen/nat))

(defn incr-by-key
  [state {:keys [key incr]}]
  (update state key (fnil + 0) incr))

(defn events-go-good
  [events]
  (let [dir (fs/temp-dir "webscale-test")]
    (try
      (let [the-thing (webscale/create incr-by-key {} dir)
            expected-end-state (reduce incr-by-key {} events)]
        (doseq [ev events] (webscale/update! the-thing ev))
        (let [in-memory-end-state @the-thing
              re-reading-end-state @(webscale/create incr-by-key {} dir)]
          (= expected-end-state
             in-memory-end-state
             re-reading-end-state)))
      (finally
        (fs/delete-dir dir)))))

(defspec basic-spec 100
  (prop/for-all [evs (gen/list gen-event)]
    (events-go-good evs)))

(defspec lots-of-events-spec 5
  (prop/for-all [evs (gen/scale #(* 10000 %) (gen/list gen-event))]
    (events-go-good evs)))
