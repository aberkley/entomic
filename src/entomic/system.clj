(ns entomic.system
  (:require [com.stuartsierra.component :as component]))

(def transactional-entities-code
  '{:lang :clojure
    :params [database id-types entities rules attributes]
    :code (let [find-id (fn [entity rule id-type]
                          (let [existing-id (ffirst
                                             (d/q '[:find ?entity
                                                    :in $ %
                                                    :where (entity? ?entity)]
                                                  database
                                                  rule))
                                temp-id (d/tempid :db.part/user)]
                            (or (:db/id entity)
                                (case id-type
                                  :update  (or existing-id temp-id)
                                  :save    (if-not existing-id temp-id)
                                  :retract existing-id
                                  :retract-entities existing-id))))
                ids (map find-id entities rules id-types)
                entities' (map (fn [entity id] (if id (assoc entity :db/id id))) entities ids)
                f (fn [id-type entity attribute]
                    (if entity
                      (case id-type
                        :retract-entities [:db.fn/retractEntity (:db/id entity)]
                        :retract [:db/retract (:db/id entity) attribute (attribute entity)]
                        entity)))
                ts (map f id-types entities' attributes)]
            (filter identity ts))})

(defn symbol-of [kw]
  (-> kw
      str
      (clojure.string/replace #":" "")
      symbol))

(defrecord Plugins [plugins]
  component/Lifecycle
  (start [component] component)
  (stop [component] component))

(defn new-plugins [plugins]
  (map->Plugins {:plugins plugins}))

(defrecord CustomFormats [parsers unparsers]
  component/Lifecycle
  (start [component] component)
  (stop [component] nil))

(defn new-custom-formats [{:keys [parsers unparsers] :as formats}]
  (let [parsers' (or parsers [])
        unparsers' (or unparsers [])]
    (map->CustomFormats {:parsers parsers'
                         :unparsers unparsers'})))

(defrecord Datomic [q db entity transact tempid function connect history as-of]
 ;;holds the datomic api so that it can be injected
  component/Lifecycle
  (start [component]
    (let [symbols (->> component
                       keys
                       (map symbol-of))]
      (reduce
       (fn [m' s]
         (assoc m' (keyword s) (ns-resolve 'datomic.api s)))
       component
       symbols)))
  (stop [component]
    ))

(defn new-datomic [datomic-api]
  (let [record (map->Datomic {})
        api-map (->> datomic-api
                     ns-interns
                     (map (fn [[k v]] [(keyword k) v]))
                     (into {}))
        initial (select-keys api-map (keys record))]
    (map->Datomic initial)))

(defrecord Entomic [datomic uri conn]
  component/Lifecycle
  (start [{:keys [uri datomic] :as component}]
    (let [{:keys [connect transact tempid function]} datomic
          conn (connect uri)]
      (transact conn [{:db/id (tempid :db.part/user)
                       :db/ident :transactional-entities
                       :db/fn (function transactional-entities-code)}])
      (assoc component :conn conn)))
  (stop [component]
    (dissoc component :conn)))

(defn new-entomic [uri datomic-api formats plugins]
  (-> (component/system-map
       :custom-formats (new-custom-formats formats)
       :datomic (new-datomic datomic-api)
       :entomic (map->Entomic {:uri uri})
       :plugins (new-plugins plugins))
      (component/system-using {:entomic [:datomic :custom-formats :plugins]})))
