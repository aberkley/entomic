(ns entomic.api
  (:require [entomic.core :only [transact! f-raw find-ids find-id] :as e]
            [entomic.format :only [parse unparse unparse-entity] :as ft]))

(def ids e/ids)

(def id e/id)

(defn f?
  [entity]
  (boolean
   (seq (e/ids entity))))

(defn fu?
  [entity]
  (= 1
     (count (e/ids entity))))

(defn f
  [partial-entity]
  (->> partial-entity
       e/f-raw
       ft/unparse))

(defn fu
  [partial-entity]
  (->> partial-entity
       e/fu-raw
       ft/unparse-entity))

(defn commit!
  [id-type f entities keys]
  (e/transact! id-type f (ft/parse entities) keys))

(defn save!
  ([entities]
     (save! entities []))
  ([entities keys]
     (commit! :save identity entities keys)))

(defn update!
  ([entities]
     (update! entities []))
  ([entities keys]
     (commit! :update identity entities keys)))

(defn- retract-transaction
  [attribute entity]
  [:db/retract (:db/id entity) attribute (attribute entity)])

(defn- retract-entity-transaction
  [entity]
  [:db.fn/retractEntity (:db/id entity)])

(defn retract!
  ([attribute entities keys]
     (commit! :retract (partial retract-transaction attribute) (filter attribute entities) keys))
  ([attribute' entities]
     (retract! attribute' entities [])))

(defn retract-entities!
  ([entities keys]
     (commit! :retract retract-entity-transaction entities keys))
  ([entities]
     (retract-entities! entities [])))
