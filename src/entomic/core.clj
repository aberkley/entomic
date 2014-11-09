(ns entomic.core
  (:use clojure.pprint
        clojure.core.incubator
        entomic.coerce
        utilities.ns))

(declare q)

(declare db)

(declare entity)

(declare transact)

(declare conn)

(declare tempid)

(declare function)

(declare connect)

(declare transactional-entities)

(declare uri)

(def datomic-api-symbols
  #{'q 'db 'entity 'transact 'conn 'tempid 'function 'connect})

(defn- intern-functions!
  [ns symbols]
  (->> ns
       ns-interns
       (filter (fn [[s f]] (contains? symbols s)))
       (map (fn [[s f]] (intern 'entomic.core s f)))
       doall))

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
  [ns-]
  (intern-functions! ns- datomic-api-symbols))

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

(def plugins (atom []))

(defn register-plugin!
  [plugin]
 (swap! plugins conj plugin))

(defn plugin-values [? form]
  (if (list? form)
   (reduce
    (fn [out plugin]
      (try (if-not out (plugin ? form))
           (catch Exception e nil)))
    nil
    @plugins)))

(defn- where-values
  [? form]
  (if-let [ps (plugin-values ? form)]
    ps
   (cond
    (vector? form) (vec
                    (map (partial where-value ?) form))
    (set? form) nil ;; or using rules
    (map? form) (entity-wheres ? form)
    (self-eval? form) [[`(= ~? ~form)]]
    :else [(where-value ? form)])))

(defn fulltext? [form]
  (try (-> form seq first (= 'fulltext))
       (catch Exception e nil)))

(defn fulltext-where [? attr form]
  (let [s (seq form)
        v (last s)]
    `[[(~'fulltext ~'$ ~attr ~v) [[~?]]]]))

(defn- entity-where
  [?entity k v]
  (cond
   (= v '?)
   [[?entity k]]
   (fulltext? v)
   (fulltext-where ?entity k v)
   :else
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
          (filter identity)))))

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

(defn nested-merge
  [m1 m2]
  (reduce
   (fn [m [k v]]
     (let [v' (if (map? v) (nested-merge (get m k) v) v)]
       (assoc m k v')))
   m1
   m2))

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
       (map (partial reduce (fn [m [ks v]] (assoc-in m ks v)) {}))
       (map (partial nested-merge entity))
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

(defn keys-in [m]
  (conj
   (if (map? m)
     (vec
      (mapcat (fn [[k v]]
                (let [sub (keys-in v)
                      nested (map #(into [k] %) (filter (comp not empty?) sub))]
                  (if (seq nested)
                    nested
                    [[k]])))
              m))
     [])
   []))

(defn extract-sets
  [entity]
  (if (set? entity)
    (let [entities (map extract-sets entity)]
      (reduce (fn [[entity' keys'] [entity keys]]
                [(merge entity' entity)
                 (into keys' keys)])
           [{} []]
           entities))
    (let [all-keys (keys-in entity)
          set-keys (filter #(set? (get-in entity %)) all-keys)
          entity' (reduce dissoc-in entity set-keys)
          sets (map (fn [ks] [ks (get-in entity ks)]) set-keys)
          sets' (map (fn [[k s]] (->> s (map (fn [v] [k v])))) sets)]
      [entity' sets'])))

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
    (let [[entity' sets] (extract-sets entity)]
    (expand-rule entity' sets))))

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

(defn find-
  [partial-entity]
  (let [database (db conn)]
    (if-let [id (:db/id partial-entity)]
      (entities (db conn) [[id]])
      (->> partial-entity
           (find-ids database)
           (entities database)))))
