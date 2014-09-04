(ns entomic.core
  (:use clojure.pprint))

;; TODO: 3 - include attribute manipulating functions

(defonce q (atom nil))

(defonce db (atom nil))

(defonce entity (atom nil))

(defonce transact (atom nil))

(defonce conn (atom nil))

(defonce tempid (atom nil))

(defonce function (atom nil))

(defonce transactional-entities (atom nil))

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
         (reset! tempid (find-api-function ns 'tempid))
         (reset! function (find-api-function ns 'function))
         (reset! transactional-entities
                 (@function '{:lang :clojure
                              :params [database q tempid id-type f entities key-queries]
                              :code (let [entity-id (fn [entity key-query]
                                                      (let [{query :query rules :rules} key-query
                                                            existing-id (ffirst (q query database rules))
                                                            temp-id (tempid :db.part/user)]
                                                        (or (:db/id entity)
                                                            (case id-type
                                                              :update  (or existing-id temp-id)
                                                              :save    (if-not existing-id temp-id)
                                                              :retract existing-id))))]
                                      (->> [entities key-queries]
                                           (apply (partial map (fn [e kq] (assoc e :db/id (entity-id e kq)))))
                                           (filter :db/id)
                                           (map f)))}))))))

(defn set-connection!
  [conn']
  (let [ident :transactional-entities]
    (do
      (reset! conn conn')
      (@transact @conn [{:db/id (@tempid :db.part/user)
                         :db/ident :transactional-entities
                         :db/fn @transactional-entities}]))))

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

(defn- entity-query-and-rules
  [entity sets]
  {:query '[:find ?entity
            :in $ %
            :where (entity? ?entity)]
   :rules (rules entity sets)})

(defn- find-ids
  [database entity]
  (let [[entity' sets] (extract-sets entity)
        {query :query rules' :rules} (entity-query-and-rules entity' sets)]
    (@q query
        database
        rules')))

(defn- find-id
  [database entity]
  (let [ids' (find-ids database entity)]
    (cond
     (= 0 (count ids')) nil
     (= 1 (count ids')) (first (first ids'))
     :else (throw (Exception. (str "more than 1 entity id found for " entity))))))

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

(defn- key-query
  [keys entity]
  {:pre (every? identity (map entity keys))}
  (let [entity' (if (seq keys)
                  (select-keys entity keys)
                  (dissoc entity :db/id))
        entity'' (if (seq entity') entity' entity)]
    (apply entity-query-and-rules (extract-sets entity''))))

(defn transact!
  [id-type f entities keys]
  (let [queries (map (partial key-query keys) entities)]
    (if (seq entities)
      (@transact @conn [[:transactional-entities @q @tempid id-type f entities queries]]))))

(defn f-raw
  [partial-entity]
  (let [database (@db @conn)]
    (if-let [id (:db/id partial-entity)]
      (entities (@db @conn) [[id]])
      (->> partial-entity
           (find-ids database)
           (entities database)))))

(defn fu-raw [x]
  "finds a single entity in datomic given a query (as an entity). Multiple results throws an exception."
  (let [entities (f-raw x)]
    (case (count entities)
      1 (first entities)
      0 nil
      (throw (Exception. (str "more than one entity found for: " x))))))

(defn ids
  [entity]
  (find-ids (@db @conn) entity))

(defn id
  [entity]
  (find-id (@db @conn) entity))
