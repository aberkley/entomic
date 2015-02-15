(ns entomic.core
  (:use [entomic.coerce]
        [entomic.rules]
        [utilities.ns]))

(defn database [{{db :db} :datomic conn :conn :as entomic}]
  (db conn))

(declare entities)

(defn find-ids
  [{:keys [datomic] :as entomic} entity]
  (let [q (:q datomic)]
    (q '[:find ?entity
         :in $ %
         :where (entity? ?entity)]
       (database entomic)
       (rule entomic entity))))

(defn find-history-ids
  [{:keys [datomic] :as entomic} entity & [attr value]]
  (let [{:keys [q history]} datomic]
    (q '[:find ?entity ?transaction
         :in $ %
         :where (entity? ?entity ?transaction)]
       (history (database entomic))
       (history-rule entomic entity))))

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

(defn entity-as-of [{:keys [datomic] :as entomic} [eid tx]]
  (let [{:keys [as-of entity]} datomic
        d (database entomic)]
      [(entity d tx)
       (entity (as-of d tx) eid)]))

(defn find-history
  [entomic partial-entity & [attr value]]
  (let [history (->> (find-history-ids entomic partial-entity)
                     (sort-by second)
                     (map (partial entity-as-of entomic))
                     (map (partial map #(zipmap (keys %) (vals %))))
                     (map (fn [[tx e]] (merge tx e))))]
    (if (and attr value)
      (filter (fn [e] (= (attr e) value)) history)
      history)))
