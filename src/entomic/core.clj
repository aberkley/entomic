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

(defn ids
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

(defn id
  [database entity]
  (let [ids (ids database entity)]
    (case (count ids)
      0 nil
      1 (first (first ids))
      :else (throw (Exception. "more than 1 entity id found")))))

(defn f?
  [database entity]
  (boolean
   (seq (ids database entity))))

(defn fu?
  [database entity]
  (let [ids (ids database entity)]
    (case (count ids)
      0 false
      1 true
      :else (throw (Exception. (str "multiple entities found for: " entity))))))

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

(comment
  (defn-db entity-to-save
    [entity keys]
    (if (:db/id entity)
      entity
      (if-let [e (fu? (select-keys entity keys))]
        nil
        (tempid :db.part/db))))

  (defn entity-to-update
    [entity keys])

  (defn-db group-entities
    [database entities unique-queries]
    (let [eqs (map (fn [e q] [e q]) entities unique-queries)
          eqs-by-id (group-by (fn [[e q]] (contains? e :db/id)) eqs)
          with-new-ids (->> (get eqs-by-id true)
                            (map first)
                            (#(or % [])))
          without-new-ids (get eqs-by-id false)
          existing-ids (->> without-new-ids
                            (map second)
                            (map #(d/q % database))
                            (map (comp first first)))
          existing-id-set (->> existing-ids
                               (filter identity)
                               (into #{}))
          with-ids (map (fn [[e q] i] (assoc e :db/id (or i (d/tempid :db.part/user)))) without-new-ids existing-ids)
          with-ids-by-existing (group-by #(contains? existing-id-set (:db/id %)) with-ids)]
      {:with-new-ids with-new-ids
       :with-temp-ids (get with-ids-by-existing false)
       :with-existing-ids (get with-ids-by-existing true)}))

  (defn-db transaction-update
    [database entities unique-queries]
    (->> (group-entities database entities unique-queries)
         vals
         flatten
         (filter identity)))

  (defn-db transaction-save
    [database entities unique-queries]
    (let [{:keys [with-new-ids with-temp-ids with-existing-ids]} (group-entities database entities unique-queries)]
      (filter identity
              (flatten [with-new-ids with-temp-ids])))))

(defn retract [attribute' entities]
  "retracts given attribute of given entities"
  (->> entities
       (filter attribute')
       (map (fn [e] [:db/retract (:db/id e) attribute' (attribute' e)]))
       (@transact conn)))

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
         (ids database)
         (entities database))))

(defn fu [x]
  "finds a single entity in datomic given a query (as an entity). Multiple results throws an exception."
  (let [entities (f x)]
    (case (count entities)
      1 (first entities)
      0 nil
      (throw (Exception. (str "more than one entity found for: " x))))))

(comment


  )

(defn retract-entities
  [entities]
  (->> entities
       (map :db/id)
       (map (fn [id] `[:db.fn/retractEntity ~id]))
       (transact conn)))