# webscale

Webscale is a stupid casual persistence thing where it stores state
changes as pretty-printed edn in regular files so you can check it
into git or something.

Don't use it for important things, it's not a good idea.

## Obtention

``` clojure
[com.gfredericks/webscale "0.1.0-SNAPSHOT"]
```

## Usage

``` clojure
(require '[com.gfredericks.webscale :as webscale])

;; a vanilla function of the sort you would use with reduce
(defn event-counts-fn
  [state ev]
  (update state (:type ev) (fnil inc 0)))

;; Here {} is the initial state
(def store (webscale/create event-counts-fn {} "data"))

(deref store)
;; => {}

(doseq [ev [{:type :a}
            {:type :b}
            {:type :a}]]
  (webscale/update! store ev))

(deref store)
;; => {:a 2, :b 1}

(doseq [ev [{:type :c}
            {:type :b}
            {:type :d}]]
  (webscale/update! store ev))

;; => {:a 2, :b 2, :c 1, :d 1}


;; You wouldn't normally have multiple stores for the same
;; configuration, but this demonstrates how it reads the events from
;; disk when you re-initialize.
(deref (webscale/create event-counts-fn {} "data"))
;; => {:a 2, :b 2, :c 1, :d 1}
```

## TODOs

- Support rolling over to new files so no file gets too large
  - Support saving snapshots of state when rolling over so
    that re-reading is faster
- Get puget integration working better for custom edn stuff

## License

Copyright © 2015 Gary Fredericks

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
