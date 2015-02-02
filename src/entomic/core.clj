(ns entomic.core
  (:use [entomic.coerce]
        [entomic.rules]
        [utilities.ns]))

(defn database [{{db :db} :datomic conn :conn :as entomic}]
  (db conn))

(declare entities)

(defprotocol Rule
  (rule [entity entomic]))

(defn query-type [_ q]
  (type q))

(defmulti rule query-type)

(defmethod rule clojure.lang.Keyword
  [entomic prefix]
  (let [q (get-in entomic [:datomic :q])]
    (->> (q '[:find ?e
              :where
              [?e :db/ident]]
            (database entomic))
         (entities entomic)
         (map :db/ident)
         (filter #(= prefix (attribute-prefix %)))
         (map prefix-rule))))

(defmethod rule :default
  [{:keys [plugins] :as entomic} entity]
  (let [[entity' sets] (extract-sets entity)]
    (expand-rule plugins entity' sets)))


(defn find-ids
  [{:keys [datomic] :as entomic} entity]
  (let [q (:q datomic)]
    (q '[:find ?entity
         :in $ %
         :where (entity? ?entity)]
       (database entomic)
       (rule entomic entity))))

(defn- decorate-entity
  [{{entity :entity} :datomic :as entomic} entity']
  (let [entity'' {:db/id (:db/id entity')}
        explicit-map (merge entity'' (zipmap (keys entity') (vals entity')))]
    (->> explicit-map
         (into [])
         (map (fn [[k v]] [k (if (:db/id v)
                              (->> (entity (database entomic) (:db/id v))
                                   (decorate-entity entomic))
                              v)]))
         (into {}))))

(defn entities
  ([{{entity :entity} :datomic :as entomic} result-set]
     (let [database' (database entomic)]
      (->> result-set
           (map first)
           (map (partial entity database'))
           (map (partial decorate-entity entomic))))))

(defn- key-rule
  [entomic entity key]
  {:pre (every? identity (map entity key))}
  (let [entity' (if (seq key)
                  (select-keys entity key)
                  (dissoc entity :db/id))
        entity'' (if (seq entity') entity' entity)]
    (rule entomic entity'')))

(defn transact!
  [{{transact :transact :as datomic} :datomic conn :conn :as entomic} id-types entities keys attributes]
  (let [rules' (map (partial key-rule entomic) entities keys)]
    (if (seq entities)
      (transact conn [[:transactional-entities id-types entities rules' attributes]]))))

(defn find-
  [entomic partial-entity]
  (if-let [id (:db/id partial-entity)]
    (entities entomic [[id]])
    (->> partial-entity
         (find-ids entomic)
         (entities entomic))))
