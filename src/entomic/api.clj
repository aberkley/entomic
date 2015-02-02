(ns entomic.api
  (:require [entomic.core :only [transact! find- find-ids] :as e]
            [entomic.format :only [parse unparse unparse-entity verify-unique] :as ft]))

(defn ids
  [entomic x]
  (->> x
       (ft/parse-entity entomic)
       (e/find-ids entomic)))

(defn id
  [entomic x]
  (->> x
       (ids entomic)
       (map first)
       (ft/verify-unique x)))

(defn f?
  [entomic x]
  (->> x
       (ids entomic)
       seq
       boolean))

(defn fu?
  [entomic x]
  (->> x
       (ids entomic)
       count
       (= 1)))

(defn f
  [entomic x]
  (->> x
       (ft/parse-entity entomic)
       (e/find- entomic)
       (ft/unparse entomic)))

(defn f
  [entomic x]
  (->> x
       (ft/parse-entity entomic)
       (e/find- entomic)
       (ft/unparse entomic)))

(defn fu
  [entomic x]
  (->> x
       (f entomic)
       (ft/verify-unique x)))

(defn- commits!
  [entomic id-types entities keys attributes]
  (e/transact! entomic id-types (ft/resolve- entomic entities) keys attributes))

(defn- expand-and-merge-args
  [cum [id-type entities key attribute]]
  (let [key' (or key [])]
    (->> entities
         (map (fn [entity] [id-type entity key' attribute]))
         (into cum))))

(defn as-transaction!
  [entomic & args]
  (apply (partial commits! entomic)
   (apply map vector
          (reduce expand-and-merge-args [] args))))

(defn save!
  ([entomic entities key]
     (as-transaction! entomic [:save entities key]))
  ([entomic entities]
     (save! entomic entities [])))

(defn update!
  ([entomic entities]
     (update! entomic entities []))
  ([entomic entities key]
     (as-transaction! entomic [:update entities key])))

(defn retract!
  ([entomic entities key attribute]
     (as-transaction! entomic [:retract (filter attribute entities) key attribute]))
  ([entomic entities attribute]
     (retract! entomic entities [] attribute)))

(defn retract-entities!
  ([entomic entities key]
     (as-transaction! entomic [:retract-entities entities key]))
  ([entomic entities]
     (retract-entities! entomic entities [])))
