(ns entomic.format
  (:use clojure.pprint)
  (:require [entomic.core :as e]
            [clj-time.coerce :as c]))

;; TODO: 1 - parse and unparse entities based on type of attribute. Ref types are resolved in the db if not an id

(defonce custom-parsers (atom {}))

(defn set-custom-parser!
  [attributes function]
  (swap! custom-parsers (partial reduce (fn [m a] (assoc m a function))) attributes))

(defn- attribute-types
  [entity]
  (let [keyset (->> entity
                    keys
                    (filter keyword?)
                    (into #{}))]
    (if (seq keyset)
      (->> keyset
           (assoc {} :db/ident)
           e/f-raw
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

(def parse-map
  {:db.type/bigdec  [string-or-number? bigdec]
   :db.type/string  [number? str]
   :db.type/bigint  [string-or-number? bigint]
   :db.type/instant [(type-match? [org.joda.time.DateTime org.joda.time.LocalDate]) c/to-date]
   :db.type/ref     [(not-type-match? [java.lang.Long clojure.lang.Keyword]) (comp :db/id e/fu-raw)]
   :db.type/keyword [string? keyword]})

(def unparse-map
  {:db.type/instant c/to-date-time})

(defn custom-parser
  [k]
  (if-let [p (get @custom-parsers k)]
    (comp :db/id p)))

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

(defn- unparser
  [d-type]
  (if d-type
    (or (d-type unparse-map)
        identity)
    identity))

(defn- unparse-value
  [d-type k v]
  ((unparser d-type) v))

(defn- modify-entity-values
  [f entity]
  (if entity
    (let [a-map (attribute-types entity)]
      (->> entity
           (into [])
           (map (fn [[k v]] [k (f (get a-map k) k v)]))
           (into {})))))

(def parse-entity (partial modify-entity-values parse-value))

(def unparse-entity (partial modify-entity-values unparse-value))

(defn parse
  [entities]
  (map parse-entity entities))

(defn unparse
  [entities]
  (map unparse-entity entities))
