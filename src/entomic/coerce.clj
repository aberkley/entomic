(ns entomic.coerce
  (:require [clojure.string :only [split] :as s]))

(defn attributes-exist?
  [attributes entity']
  (let [ks (->> (dissoc entity' :db/id) keys (into #{}))]
    (->> attributes
         (map (partial contains? ks))
         (every? identity))))

(defn attribute-prefix
  [attribute']
  {:pre (keyword? attribute')}
  (-> attribute'
      str
      (s/split #"/")
      first
      (s/split #":")
      second
      keyword))

(defn attribute-suffix
  [attribute']
  {:pre (keyword? attribute')}
  (-> attribute'
      identity
      str
      (s/split #"/")
      second
      keyword))

(defn attribute
  [prefix suffix]
  (keyword (str (name prefix) "/" (name suffix))))

(defn update-attribute-prefix
  [k new-prefix]
  (let [old-prefix (attribute-prefix k)
        suffix (attribute-suffix k)]
    (if suffix
      (attribute new-prefix suffix)
      k)))

(defn update-attribute-prefixes
  [entity new-prefix]
  (->> entity
       (into [])
       (map (fn [[k v]] [(update-attribute-prefix k new-prefix) v]))
       (into {})))

(defn consistent?
  [entity]
  (->> (dissoc entity :db/id)
       (map attribute-prefix)
       (apply =)))

(defn entity-prefix
  [entity']
  {:pre (consistent? entity')}
  (-> entity'
      (dissoc :db/id)
      keys
      first
      (attribute-prefix)))
