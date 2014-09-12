(ns entomic.api
  (:require [entomic.core :only [transact! f-raw find-ids find-id] :as e]
            [entomic.format :only [parse unparse unparse-entity] :as ft]))

(def ids e/ids)

(def id e/id)

(defn f?
  [partial-entity]
  (->> partial-entity
       ft/parse-entity
       e/ids
       seq
       boolean))

(defn fu?
  [partial-entity]
  (->> partial-entity
       ft/parse-entity
       e/ids
       count
       (= 1)))

(defn f
  [partial-entity]
  (->> partial-entity
       ft/parse-entity
       e/f-raw
       ft/unparse))

(defn fu
  [partial-entity]
  (->> partial-entity
       ft/parse-entity
       e/fu-raw
       ft/unparse-entity))

(comment
  (a/transaction!
   (:update [{:book/title "Dunes" :book/rating "8.3"} {:book/author "Frank Herberts"}] [:book/title :book/author]))

  (a/f {:book/title "Neuromancer"})

  (e/fu-raw {:user/dob #inst "1981-10-14T00:00:00.000-00:00"})

  (fu {:user/dob
           (clj-time.core/date-time 1981 10 14)})

  )


(defn- retract-transaction
  [attribute entity]
  [:db/retract (:db/id entity) attribute (attribute entity)])

(defn- retract-entity-transaction
  [entity]
  [:db.fn/retractEntity (:db/id entity)])

(defn- commits!
  [id-types fs entities keys]
  (e/transact! id-types fs (ft/parse entities) keys))

(defn- expand-and-merge-args
  [cum [id-type entities key attribute]]
  (let [key' (or key [])
        f (case id-type
            :retract-entities retract-entity-transaction
            :retract (partial retract-transaction attribute)
            identity)]
    (->> entities
         (map (fn [entity] [id-type f entity key']))
         (into cum))))

(defn as-transaction!
  [& args]
  (apply commits!
   (apply map vector
          (reduce expand-and-merge-args [] args))))

(defmacro transaction!
  [& args]
  `(apply ~as-transaction! '~args))

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
