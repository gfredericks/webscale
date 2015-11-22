(ns com.gfredericks.webscale-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.webscale :as webscale]
            [me.raynes.fs :as fs]
            [puget.printer :as puget])
  (:import (java.awt Point)
           (java.util.concurrent Executors)))

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

(defmacro with-temp-dir
  [dir-sym & body]
  `(let [~dir-sym (fs/temp-dir "webscale-test")]
     (try
       ~@body
       (finally
         (fs/delete-dir ~dir-sym)))))

(defn test-events-in-parallel
  [events nthreads]
  (with-temp-dir dir
    (let [the-thing (webscale/create incr-by-key {} dir)
          expected-end-state (reduce incr-by-key {} events)
          pool (Executors/newFixedThreadPool nthreads)]
      (try
        (let [futs
              (.invokeAll pool (map #(partial webscale/update! the-thing %) events))]
          (doseq [fut futs] (deref fut)))
        (let [in-memory-end-state @the-thing
              re-reading-end-state @(webscale/create incr-by-key {} dir)]
          (= expected-end-state
             in-memory-end-state
             re-reading-end-state))
        (finally
          (.shutdown pool))))))

(defspec parallel-events-spec 10
  (prop/for-all [evs (gen/scale #(* 40 %) (gen/list gen-event))
                 nthreads gen/s-pos-int]
    (test-events-in-parallel evs nthreads)))

(deftest special-types-test
  (with-temp-dir dir
    (let [opts {:puget-options
                {:print-handlers
                 {Point
                  (puget/tagged-handler
                   'point
                   (fn [p]
                     [(.x p) (.y p)]))}}
                :edn-options
                {:readers
                 {'point (fn [[x y]]
                           (Point. x y))}}}
          the-thing (webscale/create conj [] dir opts)
          ev1 {:my-point (Point. 2 3)}
          ev2 {:x 42 :another-point (Point. 0 0)}]
      (webscale/update! the-thing ev1)
      (is (= [ev1] @the-thing))
      (is (= [ev1] @(webscale/create conj [] dir opts)))
      (webscale/update! the-thing ev2)
      (is (= [ev1 ev2] @the-thing))
      (is (= [ev1 ev2] @(webscale/create conj [] dir opts))))))
