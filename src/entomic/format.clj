(ns entomic.format
  (:use clojure.pprint)
  (:require [entomic.core :only [find-] :as e]
            [clj-time.coerce :as c]))

;; TODO: 1 - parse and unparse entities based on type of attribute. Ref types are resolved in the db if not an id

(defn verify-unique
  [in out]
  (case (count out)
      1 (first out)
      0 nil
      (throw (Exception. (str "more than one entity found for: " in)))))

(defonce custom-parsers (atom {}))

(defonce custom-unparsers (atom {}))

(defn set-custom!
  [a attributes function]
  (swap! a (partial reduce (fn [m a] (assoc m a function))) attributes))

(def set-custom-unparser! (partial set-custom! custom-unparsers))

(def set-custom-parser! (partial set-custom! custom-parsers))

(defn- attribute-types
  [entity]
  (let [keyset (->> entity
                    keys
                    (filter keyword?)
                    (into #{}))]
    (if (seq keyset)
      (->> keyset
           (assoc {} :db/ident)
           e/find-
           (map (juxt :db/ident :db/valueType))
           (into {}))
      {})))

(defn not-type-match?
  [types]
  (fn [x]
    (->> types
         (map #(= (type x) %))
         (not-any? identity))))

(defn type-match?
  [types]
  (complement (not-type-match? types)))

(defn string-or-number?
  [x]
  (or (string? x) (number? x)))

(defn all-types? [x] true)

(defprotocol Ref
  (parse-ref [x])
  (resolve-ref [x]))

(extend-protocol Ref
  java.lang.Long
  (parse-ref [x]
    x)
  clojure.lang.Keyword
  (parse-ref [x]
    {:db/ident x})
  (resolve-ref [x]
    (resolve-ref
     (parse-ref x)))
  java.lang.Object
  (parse-ref [x]
    (->> x
         e/find-
         (verify-unique x)
         :db/id)))

(def parse-map
  {:db.type/bigdec  [string-or-number? bigdec]
   :db.type/string  [number? str]
   :db.type/bigint  [string-or-number? bigint]
   :db.type/instant [(type-match? [org.joda.time.DateTime org.joda.time.LocalDate]) c/to-date]
   :db.type/ref     [all-types? parse-ref]
   :db.type/keyword [string? keyword]})

(def unparse-map
  {:db.type/instant c/to-date-time})

(defn custom-parser
  [k]
  (get @custom-parsers k))

(defn default-parser
  [d-type k v]
  (if d-type
    (if-let [[pred parser] (d-type parse-map)]
      (if (and (keyword? k)
               (pred v))
        parser))))

(defn- parser
  [d-type k v]
  (or (custom-parser k)
      (default-parser d-type k v)
      identity))

(defn- parse-value
  [d-type k v]
  ((parser d-type k v) v))

(defn- ref-resolver
  [d-type]
  (if (= d-type :db.type/ref)
    parse-ref
    identity))

(defn- resolver
  [d-type k v]
  (comp
   (ref-resolver d-type)
   (or (parser d-type k v)
       identity)))

(defn resolve-value
  [d-type k v]
  ((resolver d-type k v) v))

(defn- custom-unparser
  [k]
  (get @custom-unparsers k))

(defn- unparser
  [d-type k]
  (if d-type
    (comp
     (or (custom-unparser k)
         identity)
     (or (d-type unparse-map)
         identity))
    identity))

(defn- unparse-value
  [d-type k v]
  ((unparser d-type k) v))

(defn- modify-entity-values
  [f entity]
  (try
   (if entity
     (let [a-map (attribute-types entity)]
       (->> entity
            (into [])
            (map (fn [[k v]] [k (f (get a-map k) k v)]))
            (into {}))))
   (catch Exception e entity)))

(def parse-entity (partial modify-entity-values parse-value))

(def unparse-entity (partial modify-entity-values unparse-value))

(def resolve-entity (partial modify-entity-values resolve-value))

(defn parse
  [entities]
  (map parse-entity entities))

(defn unparse
  [entities]
  (map unparse-entity entities))

(defn resolve-
  [entities]
  (map resolve-entity entities))
