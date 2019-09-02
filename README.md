# fressian-handlers
Fressian read/write handlers for common Clojure data types.

Extracted from https://github.com/cerner/clara-rules.

### Add to __deps.edn__:

``` clojure
fressian-handlers {:git/url "https://github.com/dimovich/fressian-handlers"
                   :sha "c99fb9e4b271add16510056a588bdba493261015"}

```

### Use like this:

```clojure
(require '[clojure.java.io :as io]
         '[clojure.data.fressian :as f]
         '[fressian.handlers :as h])


;; read one item from a fressian file
(defn read-fress [path & opts]
  (with-open [in (apply io/input-stream path opts)]
    (with-open [rdr (f/create-reader in :handlers h/read-handler-lookup)]
      (f/read-object rdr))))


;; write some data to a fressian file
(defn write-fress [path data & opts]
  (with-open [out (apply io/output-stream path opts)]
    (with-open [wrt (f/create-writer out :handlers h/write-handler-lookup)]
      (f/write-object wrt data))))


(write-fress "test.fress" ["hello" {:1 1 :2 2} #{1 2 3 4} [5 6 7 8]])
(read-fress "test.fress")
```
