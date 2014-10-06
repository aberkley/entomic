(ns entomic.core
  (:use clojure.pprint
        entomic.coerce
        utilities.ns))

(declare q)

(declare db)

(declare entity)

(declare transact)

(declare conn)

(declare tempid)

(declare function)

(declare transactional-entities)

(declare connect)

(declare uri)

(defn find-api-function
  [ns symbol']
  (->> ns
       ns-interns
       (filter (fn [[s f]] (= s symbol')))
       first
       second))

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

(defn resolve-api!
  [ns]
  (intern-ns (find-ns 'datomic.api) (find-ns 'entomic.core)))

(defn set-connection!
  [my-uri]
  (def uri my-uri)
  (def conn (connect uri))
  (transact conn [{:db/id (tempid :db.part/user)
                   :db/ident :transactional-entities
                   :db/fn (function transactional-entities-code)}]))

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

(defn- expand-rule
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

(defn prefix-rule-name
  [k]
  (-> k
      name
      (str "?")
      symbol))

(defn prefix-rule
  [ident]
  `[(~'entity? ~'?s)
    ~['?s ident]])

(declare entities)

(defprotocol Rule
  (rule [entity]))

(extend-protocol Rule
  clojure.lang.Keyword
  (rule [prefix]
    (->> (q '[:find ?e
             :where
             [?e :db/ident]]
           (db conn))
       (entities (db conn))
       (map :db/ident)
       (filter #(= prefix (attribute-prefix %)))
       (map prefix-rule)))
  java.lang.Object
  (rule [entity]
    (let [f (fn [[k v]] (set? v))
        entity' (->> entity
                     (filter (complement f))
                     (into {}))
        sets (->> entity
                  (filter f)
                  (map (fn [[k s]] (->> s (map (fn [v] [k v])))))
                  )]
    (expand-rule entity sets))))

(defn find-ids
  ([database entity]
      (q '[:find ?entity
            :in $ %
            :where (entity? ?entity)]
          database
          (rule entity)))
  ([entity]
     (find-ids (db conn) entity)))

(defn- decorate-entity
  [database entity']
  (let [entity'' {:db/id (:db/id entity')}
        explicit-map (merge entity'' (zipmap (keys entity') (vals entity')))]
    (->> explicit-map
         (into [])
         (map (fn [[k v]] [k (if (:db/id v)
                              (->> (entity database (:db/id v))
                                   (decorate-entity database))
                              v)]))
         (into {}))))

(defn entities
  ([database result-set]
     (->> result-set
          (map first)
          (map (partial entity database))
          (map (partial decorate-entity database))))
  ([result-set]
     (entities (db conn) result-set)))

(defn- key-rule
  [entity key]
  {:pre (every? identity (map entity key))}
  (let [entity' (if (seq key)
                  (select-keys entity key)
                  (dissoc entity :db/id))
        entity'' (if (seq entity') entity' entity)]
    (rule entity'')))

(defn transact!
  [id-types entities keys attributes]
  (let [rules' (map key-rule entities keys)]
    (if (seq entities)
      (transact conn [[:transactional-entities id-types entities rules' attributes]]))))

(defn find
  [partial-entity]
  (let [database (db conn)]
    (if-let [id (:db/id partial-entity)]
      (entities (db conn) [[id]])
      (->> partial-entity
           (find-ids database)
           (entities database)))))
