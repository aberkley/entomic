(ns entomic.core
  (:use clojure.pprint
        entomic.coerce))

;; TODO: 3 - include attribute manipulating functions

(def rule-query
  )

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
                              :params [database q tempid id-types fs entities key-rules]
                              :code (let [entity-id (fn [id-type f entity key-rule]
                                                      (let [existing-id (ffirst
                                                                         (q '[:find ?entity
                                                                              :in $ %
                                                                              :where (entity? ?entity)]
                                                                            database
                                                                            key-rule))
                                                            temp-id (tempid :db.part/user)]
                                                        (or (:db/id entity)
                                                            (case id-type
                                                              :update  (or existing-id temp-id)
                                                              :save    (if-not existing-id temp-id)
                                                              :retract existing-id
                                                              :retract-entities existing-id))))]
                                      (->> [id-types fs entities key-rules]
                                           (apply (partial map (fn [id-type' f' entity' key-rule']
                                                                 (let [id (entity-id id-type' f' entity' key-rule')]
                                                                   (if id (f' (assoc entity' :db/id id)))))))
                                           (filter identity)))}))))))



(comment
  (defn transactional-entities-
    [database q tempid id-types fs entities key-rules]
    (let [entity-id (fn [id-type f entity key-rule]
                      (let [existing-id (ffirst
                                         (q '[:find ?entity
                                              :in $ %
                                              :where (entity? ?entity)]
                                            database
                                            key-rule))
                            temp-id (tempid :db.part/user)]
                        (or (:db/id entity)
                            (case id-type
                              :update  (or existing-id temp-id)
                              :save    (if-not existing-id temp-id)
                              :retract existing-id
                              :retract-entities existing-id))))]
      (->> [id-types fs entities key-rules]
           (apply (partial map (fn [id-type' f' entity' key-rule']
                                 (let [id (entity-id id-type' f' entity' key-rule')]
                                   (if id (f' (assoc entity' :db/id id)))))))
           (filter identity))))

  (a/update! [] )

  (transactional-entities- (@db @conn)
                          @q
                          @tempid
                          [:update]
                          [identity]
                          [{:book/title "Dune" :book/isbn "9999999999"}]
                          [(key-rules {:book/title "Dune" :book/isbn "9999999999"} [:book/title])])

  )

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
    (->> (@q '[:find ?e
             :where
             [?e :db/ident]]
           (@db @conn))
       (entities (@db @conn))
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
      (@q '[:find ?entity
            :in $ %
            :where (entity? ?entity)]
          database
          (rule entity)))
  ([entity]
     (find-ids (@db @conn) entity)))

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

(defn entities
  ([database result-set]
     (->> result-set
          (map first)
          (map (partial @entity database))
          (map (partial decorate-entity database))))
  ([result-set]
     (entities (@db @conn) result-set)))

(defn- key-rule
  [entity key]
  {:pre (every? identity (map entity key))}
  (let [entity' (if (seq key)
                  (select-keys entity key)
                  (dissoc entity :db/id))
        entity'' (if (seq entity') entity' entity)]
    (rule entity'')))

(defn transact!
  [id-types fs entities keys]
  (let [rules' (map key-rule entities keys)]
    (if (seq entities)
      (@transact @conn [[:transactional-entities @q @tempid id-types fs entities rules']]))))

(defn find
  [partial-entity]
  (let [database (@db @conn)]
    (if-let [id (:db/id partial-entity)]
      (entities (@db @conn) [[id]])
      (->> partial-entity
           (find-ids database)
           (entities database)))))
