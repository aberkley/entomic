(ns entomic.api
  (:require [entomic.core :only [transact! find find-ids] :as e]
            [entomic.format :only [parse unparse unparse-entity verify-unique] :as ft]))

(defn ids
  [x]
  (->> x
       ft/parse-entity
       e/find-ids))

(defn id
  [x]
  (->> x
       ids
       (map first)
       (ft/verify-unique x)))

(defn f?
  [x]
  (->> x
       ids
       seq
       boolean))

(defn fu?
  [x]
  (->> x
       ids
       count
       (= 1)))

(defn f
  [x]
  (->> x
       ft/parse-entity
       e/find
       ft/unparse))

(defn fu
  [x]
  (->> x
       f
       (ft/verify-unique x)))

(defn- commits!
  [id-types entities keys attributes]
  (e/transact! id-types (ft/parse entities) keys attributes))

(defn- expand-and-merge-args
  [cum [id-type entities key attribute]]
  (let [key' (or key [])]
    (->> entities
         (map (fn [entity] [id-type entity key' attribute]))
         (into cum))))

(defn as-transaction!
  [& args]
  (apply commits!
   (apply map vector
          (reduce expand-and-merge-args [] args))))

(defn save!
  ([entities key]
     (as-transaction! [:save entities key]))
  ([entities]
     (save! entities [])))

(defn update!
  ([entities]
     (update! entities []))
  ([entities key]
     (as-transaction! [:update entities key])))

(defn retract!
  ([entities key attribute]
     (as-transaction! [:retract (filter attribute entities) key attribute]))
  ([entities attribute]
     (retract! entities [] attribute)))

(defn retract-entities!
  ([entities key]
     (as-transaction! [:retract-entities entities key]))
  ([entities]
     (retract-entities! entities [])))
