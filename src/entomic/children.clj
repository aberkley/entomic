(ns entomic.children)
(comment


  (declare extract-entities)

  (defn entity? [x]
    (and (map? x)
         (not (contains? x :part))))

  (defn- collection-of-entities? [x]
    (and (coll? x)
         (every? entity? x)))

  (defn- extract-child
    [[m ts id] [k v]]
    (cond
     (entity? v)
     [(assoc m k (e/tempid :db.part/user id))
      (conj ts (assoc v :db/id id))
      (dec id)]
     (collection-of-entities? v)
     (let [[ts' id' _] (extract-entities [] id v)
           v' (map :db/id ts')
           v'' (map (partial e/tempid :db.part/user) v')
           ]
       [(assoc m k v')
        (into ts ts')
        id'])
     :else
     [(assoc m k v)
      ts
      id]))

  (defn- extract-children
    [id entity]
    (reduce
     extract-child
     [{} [] id]
     entity))

  (defn- extract-and-merge-children
    [[es ts id] e]
    (let [[e' ts' id'] (extract-children id e)]
      [(conj es e')
       (into ts ts')
       id']))

  (defn- with-ids
    [id entities]
    (reduce
     (fn [[id' entities'] entity]
       (if (:db/id entity)
         [id' (conj entities' entity)]
         [(dec id') (conj entities' (assoc entity :db/id (e/tempid :db.part/user id')))]))
     [id []]
     entities))

  (defn- extract-entities
    [ts id entities]
    (let [[id' entities'] (with-ids id entities)
          [ts'' entities'' id''] (reduce extract-and-merge-children [[] [] id'] entities')
          ts''' (into ts ts'')
          entities''' (flatten entities'')
          out [ts''' id'' entities''']]
      (if (seq entities''')
        (apply extract-entities out)
        out)))

  (defn to-transactions [entities]
    (->> entities
         (extract-entities [] -1)
         first
         (map (fn [entity]
                (update-in entity
                           [:db/id]
                           #(if (number? %)
                              (e/tempid :db.part/user %)
                              %)))))))
