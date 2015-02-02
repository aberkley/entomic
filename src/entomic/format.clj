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

(defn- attribute-types
  [entomic entity]
  (let [keyset (->> entity
                    keys
                    (filter keyword?)
                    (into #{}))]
    (if (seq keyset)
      (->> keyset
           (assoc {} :db/ident)
           (e/find- entomic)
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
  (parse-ref [x entomic])
  (resolve-ref [x entomic]))

(defn ref-type [_ x]
  (type x))

(defmulti parse-ref ref-type)

(defmethod parse-ref :default
  [entomic x]
  x)

(defmethod parse-ref clojure.lang.Keyword
  [entomic x]
  {:db/ident x})

(defmulti resolve-ref ref-type)

(defmethod resolve-ref clojure.lang.Keyword
  [entomic x]
  (resolve-ref entomic
   (parse-ref entomic x)))

(defmethod resolve-ref :default
  [entomic x]
    (->> x
         (e/find- entomic)
         (verify-unique x)
         :db/id))

(defn parse-map [entomic]
  {:db.type/bigdec  [string-or-number? bigdec]
   :db.type/string  [number? str]
   :db.type/bigint  [string-or-number? bigint]
   :db.type/instant [(type-match? [org.joda.time.DateTime org.joda.time.LocalDate]) c/to-date]
   :db.type/ref     [all-types? (partial parse-ref entomic)]
   :db.type/keyword [string? keyword]})

(def unparse-map
  {:db.type/instant c/to-date-time})

(defn default-parser
  [entomic d-type k v]
  (if d-type
    (if-let [[pred parser] (d-type (parse-map entomic))]
      (if (and (keyword? k)
               (pred v))
        parser))))

(defn- parse-value
  [{{parsers :parsers} :custom-formats :as entomic} d-type k v]
  (if-let [f (get parsers k)]
    (f v)
    (if-let [f (default-parser entomic d-type k v)]
      (f v)
      v)))

(defn- resolve-value
  [{{parsers :parsers} :custom-formats :as entomic} d-type k v]
  (let [v' (if-let [f (get parsers k)] (f v) v)]
   (if (= d-type :db.type/ref)
     (resolve-ref entomic v')
     (if-let [f (default-parser entomic d-type k v')]
       (f v')
       v'))))

(defn- unparse-value
  [{{unparsers :unparsers} :custom-formats} d-type k v]
  (let [v' (if-let [u (get unparse-map d-type)] (u v) v)]
    (if-let [u (get unparsers k)] (u v') v')))

(defn- modify-entity-values
  [f entomic entity]
  (if (or (keyword? entity)
          (set? entity)
          (nil? entity))
    entity
    (let [a-map (attribute-types entomic entity)]
      (->> entity
           (into [])
           (map (fn [[k v]] [k (f entomic (get a-map k) k v)]))
           (into {})))))

(def parse-entity (partial modify-entity-values parse-value))

(def unparse-entity (partial modify-entity-values unparse-value))

(def resolve-entity (partial modify-entity-values resolve-value))

(defn parse
  [entomic entities]
  (map (partial parse-entity entomic) entities))

(defn unparse
  [entomic entities]
  (map (partial unparse-entity entomic) entities))

(defn resolve-
  [entomic entities]
  (map (partial resolve-entity entomic) entities))
