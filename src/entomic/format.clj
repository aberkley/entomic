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

(def type-map
  {:db.type/bigdec  [java.math.BigDecimal bigdec]
   :db.type/string  [java.lang.String str]
   :db.type/bigint  [clojure.lang.BigInt bigint]
   :db.type/instant [java.util.Date c/to-date]
   :db.type/ref     [java.lang.Long (comp :db/id e/fu-raw)]})

(defn- custom-parse-value
  [a-map k v]
  (if-let [f (@custom-parsers k)]
    (let [v' ((k @custom-parsers) v)]
     (if-let [id (:db/id v')]
       id
       v'))
    v))

(defn- parse-value
  [a-map k v]
  (let [d-type (k a-map)
        [t f] (if d-type (type-map d-type) [nil identity])
        f'    (cond
               (k @custom-parsers) (comp :db/id (k @custom-parsers))
               (= (type v) t) identity
               :else f)]
    (if f'
      (f' v)
      v)))

(defn- unparse-value
  [a-map k v]
  (if (= (k a-map) :db.type/instant)
    (c/to-date-time v)
    v))

(defn- modify-entity-values
  [f entity]
  (if entity
    (let [a-map (attribute-types entity)]
      (->> entity
           (into [])
           (map (fn [[k v]] [k (f a-map k v)]))
           (into {})))))

(def custom-parse-entity (partial modify-entity-values custom-parse-value))

(def parse-entity (partial modify-entity-values parse-value))

(def unparse-entity (partial modify-entity-values unparse-value))

(defn parse
  [entities]
  (map parse-entity entities))

(defn unparse
  [entities]
  (map unparse-entity entities))
