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
    (rules plugins entity' sets)))

(defn find-history-ids
  [{:keys [datomic] :as entomic} entity]
  (let [{:keys [q history]} datomic]
    (q '[:find ?entity ?transaction
         :in $ %
         :where (entity? ?entity ?transaction)]
       (history (database entomic))
       (history-rules entomic entity))))

(comment
  (entity-wheres {} '?entity {:book/author "George Orwell"})
  (history-rules {} {:book/author "George Orwell"} [])

  (create-history)

  (find-history-ids entomic.test/entomic {:book/author "Iain M. Banks"})

  (defn entity-as-of [[eid tx]]
    (let [database (d/db (d/connect uri))]
      [(d/entity database tx)
       (d/entity (d/as-of database tx) eid)]))

  (->> (d/q
        '[:find ?tx ?e
          :in $
          :where
          [?e :book/author "George Orwell"]
          [?e :book/isbn "2" ?tx]]
        (d/history (d/db (d/connect uri))))
       sort
       ;;(mapv #(d/entity (d/as-of (d/db (d/connect uri)) (first %)) eid))
       (map entity-as-of)
       (map (partial map #(zipmap (keys %) (vals %))))
       (map (fn [[tx e]] (merge tx e)))
       (filter #(= "2" (:book/isbn %)))
       )

  )

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
