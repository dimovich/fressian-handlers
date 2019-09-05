(ns fressian.handlers
  (:require [clojure.data.fressian :as fres]
            [clojure.java.io :as io]
            [clojure.main :as cm])
  
  (:import [org.fressian
            StreamingWriter
            Writer
            Reader
            FressianWriter
            FressianReader]
           [org.fressian.handlers
            WriteHandler
            ReadHandler]
           [java.util
            List
            ArrayList
            IdentityHashMap
            Map
            WeakHashMap]
           [java.io
            InputStream
            OutputStream]))


;; taken from
;; https://github.com/cerner/clara-rules/blob/master/src/main/clojure/clara/rules/durability/fressian.clj




;; ctx
#_"A cache for writing and reading Clojure records.  At write time, an IdentityHashMap can be
   used to keep track of repeated references to the same object instance occurring in
   the serialization stream.  At read time, a plain ArrayList (mutable and indexed for speed)
   can be used to add records to when they are first seen, then look up repeated occurrences
   of references to the same record instance later."


(defn clj-struct->idx
  "Gets the numeric index for the given struct from the clj-struct-holder."
  [ctx fact]
  (.get ctx fact))

(defn clj-struct-holder-add-fact-idx!
  "Adds the fact to the clj-struct-holder with a new index.  This can later be retrieved
   with clj-struct->idx."
  [ctx fact]
  ;; Note the values will be int type here.  This shouldn't be a problem since they
  ;; will be read later as longs and both will be compatible with the index lookup
  ;; at read-time.  This could have a cast to long here, but it would waste time
  ;; unnecessarily.
  (.put ctx fact (.size ctx)))

(defn clj-struct-idx->obj
  "The reverse of clj-struct->idx.  Returns an object for the given index found
   in clj-struct-holder."
  [ctx id]
  (.get ctx id))

(defn clj-struct-holder-add-obj!
  "The reverse of clj-struct-holder-add-fact-idx!.  Adds the object to the clj-struct-holder
   at the next available index."
  [ctx fact]
  (.add ctx fact)
  fact)

(defn create-map-entry
  "Helper to create map entries.  This can be useful for serialization implementations
   on clojure.lang.MapEntry types.
   Using the ctor instead of clojure.lang.MapEntry/create since this method
   doesn't exist prior to clj 1.8.0"
  [k v]
  (clojure.lang.MapEntry. k v))

;;;; To deal with http://dev.clojure.org/jira/browse/CLJ-1733 we need to impl a way to serialize
;;;; sorted sets and maps.  However, this is not sufficient for arbitrary comparators.  If
;;;; arbitrary comparators are used for the sorted coll, the comparator has to be restored
;;;; explicitly since arbitrary functions are not serializable in any stable way right now.

(defn sorted-comparator-name
  "Sorted collections are not easily serializable since they have an opaque function object instance
   associated with them.  To deal with that, the sorted collection can provide a ::comparator-name 
   in the metadata that indicates a symbolic name for the function used as the comparator.  With this
   name the function can be looked up and associated to the sorted collection again during
   deserialization time.
   * If the sorted collection has metadata ::comparator-name, then the value should be a name 
   symbol and is returned.  
   * If the sorted collection has the clojure.lang.RT/DEFAULT_COMPARATOR, returns nil.
   * If neither of the above are true, an exception is thrown indicating that there is no way to provide
   a useful name for this sorted collection, so it won't be able to be serialized."
  [^clojure.lang.Sorted s]
  (let [cname (-> s meta ::comparator-name)]

    ;; Fail if reliable serialization of this sorted coll isn't possible.
    (when (and (not cname)
               (not= (.comparator s) clojure.lang.RT/DEFAULT_COMPARATOR))
      (throw (ex-info (str "Cannot serialize sorted collection with non-default"
                           " comparator because no :clara.rules.durability/comparator-name provided in metadata.")
                      {:sorted-coll s
                       :comparator (.comparator s)})))

    cname))

(defn seq->sorted-set
  "Helper to create a sorted set from a seq given an optional comparator."
  [s ^java.util.Comparator c]
  (if c
    (clojure.lang.PersistentTreeSet/create c (seq s))
    (clojure.lang.PersistentTreeSet/create (seq s))))

(defn seq->sorted-map
  "Helper to create a sorted map from a seq given an optional comparator."
  [s ^java.util.Comparator c]
  (if c
    (clojure.lang.PersistentTreeMap/create c ^clojure.lang.ISeq (sequence cat s))
    (clojure.lang.PersistentTreeMap/create ^clojure.lang.ISeq (sequence cat s))))





;; Use this map to cache the symbol for the map->RecordNameHere
;; factory function created for every Clojure record to improve
;; serialization performance.
;; See https://github.com/cerner/clara-rules/issues/245 for more extensive discussion.
(def ^:private ^Map class->factory-fn-sym (java.util.Collections/synchronizedMap
                                           (WeakHashMap.)))

(defn record-map-constructor-name
  "Return the 'map->' prefix, factory constructor function for a Clojure record."
  [rec]
  (let [klass (class rec)]
    (if-let [cached-sym (.get class->factory-fn-sym klass)]
      cached-sym
      (let [class-name (.getName ^Class klass)
            idx (.lastIndexOf class-name (int \.))
            ns-nom (.substring class-name 0 idx)
            nom (.substring class-name (inc idx))
            factory-fn-sym (symbol (str (cm/demunge ns-nom)
                                        "/map->"
                                        (cm/demunge nom)))]
        (.put class->factory-fn-sym klass factory-fn-sym)
        factory-fn-sym))))

(defn write-map
  "Writes a map as Fressian with the tag 'map' and all keys cached."
  [^Writer w m]
  (.writeTag w "map" 1)
  (.beginClosedList ^StreamingWriter w)
  (reduce-kv
   (fn [^Writer w k v]
     (.writeObject w k true)
     (.writeObject w v))
   w
   m)
  (.endList ^StreamingWriter w))

(defn write-with-meta
  "Writes the object to the writer under the given tag.  If the record has metadata, the metadata
   will also be written.  read-with-meta will associated this metadata back with the object
   when reading."
  ([w tag o]
   (write-with-meta w tag o (fn [^Writer w o] (.writeList w o))))
  ([^Writer w tag o write-fn]
   (let [m (meta o)]
     (do
       (.writeTag w tag 2)
       (write-fn w o)
       (if m
         (.writeObject w m)
         (.writeNull w))))))

(defn- read-meta [^Reader rdr]
  (some->> rdr
           .readObject
           (into {})))

(defn read-with-meta
  "Reads an object from the reader that was written via write-with-meta.  If the object was written
   with metadata the metadata will be associated on the object returned."
  [^Reader rdr build-fn]
  (let [o (build-fn (.readObject rdr))
        m (read-meta rdr)]
    (cond-> o
      m (with-meta m))))

(defn write-record
  "Same as write-with-meta, but with Clojure record support.  The type of the record will
   be preserved."
  [^Writer w tag rec]
  (let [m (meta rec)]
    (.writeTag w tag 3)
    (.writeObject w (record-map-constructor-name rec) true)
    (write-map w rec)
    (if m
      (.writeObject w m)
      (.writeNull w))))

(defn read-record
  "Same as read-with-meta, but with Clojure record support.  The type of the record will
   be preserved."
  ([^Reader rdr]
   (read-record rdr nil))
  ([^Reader rdr add-fn]
   (let [builder (-> (.readObject rdr) resolve deref)
         build-map (.readObject rdr)
         m (read-meta rdr)]
     (cond-> (builder build-map)
       m (with-meta m)
       add-fn add-fn))))


(defn create-handler
  [clazz
   tag
   write-fn
   read-fn]
  {:class clazz
   :writer (reify WriteHandler
             (write [_ w o]
               (write-fn w tag o)))
   :readers {tag
             (reify ReadHandler
               (read [_ rdr _ _]
                 (read-fn rdr)))}})



(defn create-identity-based-handler
  [clazz
   tag
   write-fn
   read-fn
   ctx]
  (let [indexed-tag (str tag "-idx")]
    ;; Write an object a single time per object reference to that object.  The object is then "cached"
    ;; with the IdentityHashMap `(:clj-struct ctx)`.  If another reference to this object instance
    ;; is encountered later, only the "index" of the object in the map will be written.
    {:class clazz
     :writer (reify WriteHandler
               (write [_ w o]
                 (if-let [idx (clj-struct->idx ctx o)]
                   (do
                     (.writeTag w indexed-tag 1)
                     (.writeInt w idx))
                   (do
                     ;; We are writing all nested objects prior to adding the original object to the cache here as
                     ;; this will be the order that will occur on read, ie, the reader will have traverse to the bottom
                     ;; of the struct before rebuilding the object.
                     (write-fn w tag o)
                     (clj-struct-holder-add-fact-idx! ctx o)))))
     ;; When reading the first time a reference to an object instance is found, the entire object will
     ;; need to be constructed.  It is then put into indexed cache.  If more references to this object
     ;; instance are encountered later, they will be in the form of a numeric index into this cache.
     ;; This is guaranteed by the semantics of the corresponding WriteHandler.
     :readers {indexed-tag
               (reify ReadHandler
                 (read [_ rdr _ _]
                   (clj-struct-idx->obj ctx (.readInt rdr))))
               tag
               (reify ReadHandler
                 (read [_ rdr _ _]
                   (->> rdr
                        read-fn
                        (clj-struct-holder-add-obj! ctx))))}}))




(defn get-handlers
  "A structure tying together the custom Fressian write and read handlers used
   by FressianSessionSerializer's."
  [ctx]
  {"java/class"
   {:class Class
    :writer (reify WriteHandler
              (write [_ w c]
                (.writeTag w "java/class" 1)
                (.writeObject w (symbol (.getName ^Class c)) true)))
    :readers {"java/class"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (resolve (.readObject rdr))))}}

   "clj/set"
   (create-identity-based-handler
    clojure.lang.APersistentSet
    "clj/set"
    write-with-meta
    (fn clj-set-reader [rdr] (read-with-meta rdr set))
    ctx)
   
   "clj/vector"
   (create-identity-based-handler
    clojure.lang.APersistentVector
    "clj/vector"
    write-with-meta
    (fn clj-vec-reader [rdr] (read-with-meta rdr vec))
    ctx)

   "clj/list"
   (create-identity-based-handler
    clojure.lang.PersistentList
    "clj/list"
    write-with-meta
    (fn clj-list-reader [rdr] (read-with-meta rdr #(apply list %)))
    ctx)

   "clj/emptylist"
   ;; Not using the identity based handler as this will always be identical anyway
   ;; then meta data will be added in the reader
   {:class clojure.lang.PersistentList$EmptyList
    :writer (reify WriteHandler
              (write [_ w o]
                (let [m (meta o)]
                  (do
                    (.writeTag w "clj/emptylist" 1)
                    (if m
                      (.writeObject w m)
                      (.writeNull w))))))
    :readers {"clj/emptylist"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (let [m (read-meta rdr)]
                    (cond-> '()
                      m (with-meta m)))))}}

   "clj/aseq"
   (create-identity-based-handler
    clojure.lang.ASeq
    "clj/aseq"
    write-with-meta
    (fn clj-seq-reader [rdr] (read-with-meta rdr sequence))
    ctx)

   "clj/lazyseq"
   (create-identity-based-handler
    clojure.lang.LazySeq
    "clj/lazyseq"
    write-with-meta
    (fn clj-lazy-seq-reader [rdr] (read-with-meta rdr sequence))
    ctx)

   "clj/map"
   (create-identity-based-handler
    clojure.lang.APersistentMap
    "clj/map"
    (fn clj-map-writer [wtr tag m] (write-with-meta wtr tag m write-map))
    (fn clj-map-reader [rdr] (read-with-meta rdr #(into {} %)))
    ctx)

   "clj/treeset"
   (create-identity-based-handler
    clojure.lang.PersistentTreeSet
    "clj/treeset"
    (fn clj-treeset-writer [^Writer wtr tag s]
      (let [cname (sorted-comparator-name s)]
        (.writeTag wtr tag 3)
        (if cname
          (.writeObject wtr cname true)
          (.writeNull wtr))
        ;; Preserve metadata.
        (if-let [m (meta s)]
          (.writeObject wtr m)
          (.writeNull wtr))
        (.writeList wtr s)))
    (fn clj-treeset-reader [^Reader rdr]
      (let [c (some-> rdr .readObject resolve deref)
            m (.readObject rdr)
            s (-> (.readObject rdr)
                  (seq->sorted-set c))]
        (if m
          (with-meta s m)
          s)))
    ctx)

   "clj/treemap"
   (create-identity-based-handler
    clojure.lang.PersistentTreeMap
    "clj/treemap"
    (fn clj-treemap-writer [^Writer wtr tag o]
      (let [cname (sorted-comparator-name o)]
        (.writeTag wtr tag 3)
        (if cname
          (.writeObject wtr cname true)
          (.writeNull wtr))
        ;; Preserve metadata.
        (if-let [m (meta o)]
          (.writeObject wtr m)
          (.writeNull wtr))
        (write-map wtr o)))
    (fn clj-treemap-reader [^Reader rdr]
      (let [c (some-> rdr .readObject resolve deref)
            m (.readObject rdr)
            s (seq->sorted-map (.readObject rdr) c)]
        (if m
          (with-meta s m)
          s)))
    ctx)

   "clj/mapentry"
   (create-identity-based-handler
    clojure.lang.MapEntry
    "clj/mapentry"
    (fn clj-mapentry-writer [^Writer wtr tag o]
      (.writeTag wtr tag 2)
      (.writeObject wtr (key o) true)
      (.writeObject wtr (val o)))
    (fn clj-mapentry-reader [^Reader rdr]
      (create-map-entry (.readObject rdr)
                        (.readObject rdr)))
    ctx)

   ;; Have to redefine both Symbol and IRecord to support metadata as well
   ;; as identity-based caching for the IRecord case.

   "clj/sym"
   (create-identity-based-handler
    clojure.lang.Symbol
    "clj/sym"
    (fn clj-sym-writer [^Writer wtr tag o]
      ;; Mostly copied from private fres/write-named, except the metadata part.
      (.writeTag wtr tag 3)
      (.writeObject wtr (namespace o) true)
      (.writeObject wtr (name o) true)
      (if-let [m (meta o)]
        (.writeObject wtr m)
        (.writeNull wtr)))
    (fn clj-sym-reader [^Reader rdr]
      (let [s (symbol (.readObject rdr) (.readObject rdr))
            m (read-meta rdr)]
        (cond-> s
          m (with-meta m))))
    ctx)

   "clj/record"
   (create-identity-based-handler
    clojure.lang.IRecord
    "clj/record"
    write-record
    read-record
    ctx)})

(defn get-write-handlers
  "All Fressian write handlers used by FressianSessionSerializer's."
  [& [ctx]]
  (into fres/clojure-write-handlers
        (map (fn [[tag {clazz :class wtr :writer}]]
               [clazz {tag wtr}]))
        (get-handlers (or ctx (IdentityHashMap.)))))

(defn get-read-handlers
  "All Fressian read handlers used by FressianSessionSerializer's."
  [& [ctx]]
  (->> (get-handlers (or ctx (ArrayList.)))
       vals
       (into fres/clojure-read-handlers
             (mapcat :readers))))

(defn get-write-handler-lookup
  [& [ctx]]
  (-> (get-write-handlers ctx)
      fres/associative-lookup
      fres/inheritance-lookup))

(defn get-read-handler-lookup
  [& [ctx]]
  (fres/associative-lookup (get-read-handlers ctx)))
