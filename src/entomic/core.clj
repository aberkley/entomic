(ns entomic.core
  (:use clojure.pprint))

;; TODO: 1 - parse and unparse entities based on type of attribute. Ref types are resolved in the db if not an id
;; TODO: 2 - write update/save (as u/s) where key attributes are passed in (no extended schema)
;; TODO: 3 - include attribute manipulating functions

(defonce q (atom nil))

(defonce db (atom nil))

(defonce entity (atom nil))

(defonce transact (atom nil))

(defonce conn (atom nil))

(defonce tempid (atom nil))

(defn find-api-function
  [ns symbol']
  (->> ns
       ns-interns
       (filter (fn [[s f]] (= s symbol')))
       first
       second))

(defn resolve-api!
  ([ns]
     (if ns
       (do
         (reset! q (find-api-function ns 'q))
         (reset! db (find-api-function ns 'db))
         (reset! entity (find-api-function ns 'entity))
         (reset! transact (find-api-function ns 'transact))
         (reset! conn (find-api-function ns 'conn))
         (reset! tempid (find-api-function ns 'tempid))))))

(defn set-connection!
  [conn']
  (reset! conn conn'))

(defn- find-attribute
  [form]
  (if (keyword? form)
    form
    (second (seq form))) )

(defn- self-eval?
  [form]
  (boolean
   (try (= (eval form) form)
        (catch Exception e nil))))

(defn- where-value
  [? form]
  (let [form' (seq form)]
   [(seq [(first form') ? (second form')])]))

(defn- entity-wheres
  [?entity k v])

(defn- where-values
  [? form]
  (cond
   (vector? form) (vec
                   (map (partial where-value ?) form))
   (set? form) nil ;; or using rules
   (map? form) (entity-wheres ? form)
   (self-eval? form) [[`(= ~? ~form)]]
   :else [(where-value ? form)]))

(defn- entity-where
  [?entity k v]
  (let [k? (self-eval? k)
        a (find-attribute k)
        ? (symbol (str '? (name a)))
        ?' (symbol (str '? (name a) "'"))
        where-key (if (not k?)
                    [(clojure.walk/prewalk-replace {a ?} k) ?'])
        val-sym (if k? ? ?')
        where-val  (where-values val-sym v)]
    (->> [?entity a ?]
         (merge where-val where-key)
         (filter identity))))

(defn- entity-wheres
  [?entity entity]
  (->> (dissoc entity :db/id)
         (into [])
         (map (partial apply (partial entity-where ?entity)))
         (reduce (fn [v w] (into v w)) [])))

(defn- extract-sets
  [entity]
  (let [f (fn [[k v]] (set? v))
        entity' (->> entity
                     (filter (complement f))
                     (into {}))
        sets (->> entity
                  (filter f)
                  (map (fn [[k s]] (->> s (map (fn [v] [k v])))))
                  )]
    [entity' sets]))

(defn- rules
  [entity sets]
  (->> sets
       (reduce
        (fn [agg new]
          (if (seq agg)
            (for [n new a agg]
              (merge a n))
            new))
        [[]])
       (map (partial into {}))
       (map (partial merge entity))
       (map (partial entity-wheres '?entity))
       (map (fn [rule] (into '[[entity? ?entity]] rule)))
       vec))

(defn- entity-query
  [entity]
  (into [:find '?entity :where]
        (entity-wheres '?entity entity)))

(defn- find-ids
  [database entity]
  (let [[entity' sets] (extract-sets entity)]
    (if (seq sets)
      (@q '[:find ?entity
            :in $ %
            :where (entity? ?entity)]
          database
          (rules entity' sets))
      (-> (entity-query entity)
          (@q database)))))

(defn- find-id
  [database entity]
  (let [ids' (find-ids database entity)]
    (cond
     (= 0 (count ids')) nil
     (= 1 (count ids')) (first (first ids'))
     :else (throw (Exception. (str "more than 1 entity id found for " entity))))))

(defn ids
  [entity]
  (find-ids (@db @conn) entity))

(defn id
  [entity]
  (find-id (@db @conn) entity))

(defn f?
  [entity]
  (boolean
   (seq (ids entity))))

(defn fu?
  [entity]
  (boolean
   (find-id (@db @conn) entity)))

(defn- decorate-entity
  [database entity']
  (let [entity'' {:db/id (:db/id entity')}
        explicit-map (merge entity'' (zipmap (keys entity') (vals entity')))]
    (->> explicit-map
         (into [])
         (map (fn [[k v]] [k (if (:db/id v)
                              (->> (@entity database (:db/id v))
                                   (decorate-entity database))
                              v)]))
         (into {}))))

(defn- entities
  [database result-set]
  (->> result-set
       (map first)
       (map (partial @entity database))
       (map (partial decorate-entity database))))

(defn- key-id
  [database entity keys]
  {:pre (every? identity (map entity keys))}
  (let [entity' (if (seq keys)
                  (select-keys entity keys)
                  (dissoc entity :db/id))]
    (if (seq entity')
      (find-id database entity')
      (find-id database entity))))

(defn- entity-id
  [database include-existing? keys entity]
 (if-let [id' (:db/id entity)]
    id'
    (if-let [id'' (key-id database entity keys)]
      (if include-existing? id'' nil)
      (@tempid :db.part/user))))

(defn- merge-entity-ids
  [database include-existing? keys entities]
  (->> entities
       (map #(assoc % :db/id (entity-id database include-existing? keys %)))
       (filter :db/id)))

(defn- commit!
  [include-existing? entities keys]
  (let [entities' (merge-entity-ids (@db @conn) include-existing? keys entities)]
    (if (seq entities')
      (@transact @conn entities'))))

(defn- dispatch-find
  [x]
  (cond
   (:db/id x) :id
   (map? x)   :partial-entity))

(defmulti f "finds entities given a query (as an entity)" dispatch-find)

(defmethod f :id
  [{id :db/id}]
  (entities (@db @conn) [[id]]))

(defmethod f :partial-entity
  [partial-entity]
  (let [database (@db @conn)]
    (->> partial-entity
         (find-ids database)
         (entities database))))

(defn fu [x]
  "finds a single entity in datomic given a query (as an entity). Multiple results throws an exception."
  (let [entities (f x)]
    (case (count entities)
      1 (first entities)
      0 nil
      (throw (Exception. (str "more than one entity found for: " x))))))

(defn save!
  ([entities]
     (save! entities []))
  ([entities keys]
     (commit! false entities keys)))

(defn update!
  ([entities]
     (update! entities []))
  ([entities keys]
     (commit! true entities keys)))

(defn retract
  ([attribute' entities keys]
     "retracts given attribute of given entities"
     (let [database (@db @conn)]
       (->> entities
            (filter attribute')
            (merge-entity-ids database true keys)
            (map (fn [e] [:db/retract (:db/id e) attribute' (attribute' e)]))
            (@transact @conn))))
  ([attribute' entities]
     (retract attribute' entities [])))

(defn retract-entities
  ([entities keys]
      (let [database (@db @conn)]
        (->> entities
             (merge-entity-ids database true keys)
             (map :db/id)
             (map (fn [id] `[:db.fn/retractEntity ~id]))
             (@transact @conn))))
  ([entities]
     (retract-entities entities [])))
