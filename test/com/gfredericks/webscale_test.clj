(ns com.gfredericks.webscale-test
  (:require [bond.james :as bond]
            [clojure.test :refer :all]
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
  [events opts]
  (let [dir (fs/temp-dir "webscale-test")]
    (try
      (let [make-thing #(webscale/create incr-by-key {} dir opts)
            the-thing (make-thing)
            expected-end-state (reduce incr-by-key {} events)]
        (doseq [ev events] (webscale/update! the-thing ev))
        (let [in-memory-end-state @the-thing
              re-reading-end-state @(make-thing)]
          (= expected-end-state
             in-memory-end-state
             re-reading-end-state)))
      (finally
        (fs/delete-dir dir)))))

(def gen-opts
  (gen/hash-map :max-file-size (gen/large-integer* {:min 1 :max 10000000})
                :cache-state? gen/boolean))

(defspec basic-spec 100
  (prop/for-all [evs (gen/list gen-event)
                 opts gen-opts]
    (events-go-good evs opts)))

(defspec lots-of-events-spec 5
  (prop/for-all [evs (gen/scale #(* 10000 %) (gen/list gen-event))
                 opts gen-opts]
    (events-go-good evs opts)))

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

(deftype Box [x]
  Object
  (equals [_ y]
    (and (instance? Box y) (= x (.x y)))))

(defmethod print-method Box
  [box pw]
  (.write pw "#user/box ")
  (print-method (.x box) pw))

(deftest special-types-test
  (testing "with explicit print-handlers"
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
  (testing "with print-fallback"
    (with-temp-dir dir
      (let [opts {:edn-options
                  {:readers
                   {'user/box (fn [x] (Box. x))}}}
            the-thing (webscale/create conj [] dir opts)
            ev1 {:my-box (Box. "24")}
            ev2 {:x 42 :another-box (Box. [19])}]
        (webscale/update! the-thing ev1)
        (is (= [ev1] @the-thing))
        (is (= [ev1] @(webscale/create conj [] dir opts)))
        (webscale/update! the-thing ev2)
        (is (= [ev1 ev2] @the-thing))
        (is (= [ev1 ev2] @(webscale/create conj [] dir opts)))))))

(deftest caching-test
  (with-temp-dir dir
    (let [make-thing #(webscale/create #'incr-by-key {} dir
                                       {:max-file-size 100
                                        :cache-state? true})
          the-thing (make-thing)]
      (dotimes [n 10000]
        (let [k ('[a b c d e f] (mod n 6))]
          (webscale/update! the-thing {:key k :incr (mod (* n 17) 19)})))
      (is (= @the-thing '{a 15001, b 15011, c 15002, d 14993, e 14996, f 15008}))
      (bond/with-spy [incr-by-key]
        (is (= @(make-thing) @the-thing))
        (is (<= (-> incr-by-key bond/calls count) 100)
            "Caching the state means we don't have to call the update function many times."))
      (testing "cache files can be regenerated on create"
        (let [slurp-cache-state
              (fn []
                (->> (fs/list-dir dir)
                     (filter #(re-matches #".*\.cache\.edn" (str %)))
                     (map (fn [file]
                            [file (slurp file)]))
                     (into {})))

              current-cache-state (slurp-cache-state)]
          (is (< 1000 (count current-cache-state)))
          (run! fs/delete (keys current-cache-state))
          (is (= {} (slurp-cache-state)))
          (make-thing)
          (is (= current-cache-state (slurp-cache-state))))))))
