(ns entomic.rules
  (:use [entomic.coerce])
  (:require [clojure.core.incubator :refer :all]
            [clojure.walk :refer :all]))

(defn plugin-values [plugins ? form]
  (if (list? form)
   (reduce
    (fn [out plugin]
      (try (if-not out (plugin ? form))
           (catch Exception e nil)))
    nil
    plugins)))

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

(declare entity-wheres)

(defn- where-value
  [? form]
  (let [form' (seq form)]
   [(seq [(first form') ? (second form')])]))

(defn- where-values
  [plugins ? form]
  (if-let [ps (plugin-values plugins ? form)]
    ps
   (cond
    (vector? form) (vec
                    (map (partial where-value ?) form))
    (set? form) nil ;; or using rules
    (map? form) (entity-wheres plugins ? form)
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
  [plugins ?entity k v]
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
                     [(prewalk-replace {a ?} k) ?'])
         val-sym (if k? ? ?')
         where-val  (where-values plugins val-sym v)]
     (->> [?entity a ?]
          (merge where-val where-key)
          (filter identity)))))

(defn- entity-wheres
  [plugins ?entity entity]
  (->> (dissoc entity :db/id)
         (into [])
         (map (partial apply (partial entity-where plugins ?entity)))
         (reduce (fn [v w] (into v w)) [])))

(defn- nested-merge
  [m1 m2]
  (reduce
   (fn [m [k v]]
     (let [v' (if (map? v) (nested-merge (get m k) v) v)]
       (assoc m k v')))
   m1
   m2))

(defn- expand-rule
  [{:keys [plugins]} entity sets]
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
       (map (partial entity-wheres plugins '?entity))))

(defn rules [plugins entity sets]
  (->> (expand-rule plugins entity sets)
       (map (fn [rule] (into '[[entity? ?entity]] rule)))
       vec))

(defn history-rules [plugins entity sets tx-clause]
  (->> (expand-rule plugins entity sets)
       (map (fn [rule] (into rule tx-clause)))
       (map (fn [rule] (into '[[entity? ?entity ?transaction]] rule)))
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

(defn query-type [_ q]
  (type q))

(defmulti rule query-type)

(defmethod rule clojure.lang.Keyword
  [{:keys [datomic conn]} prefix]
  (let [{:keys [q entity db]} datomic
        database (db conn)]
    (->> (q '[:find ?e
              :where
              [?e :db/ident]]
            database)
         identity
         (map first)
         (map (partial entity database))
         (map :db/ident)
         (filter #(= prefix (attribute-prefix %)))
         (map prefix-rule))))

(defmethod rule :default
  [{:keys [plugins] :as entomic} entity]
  (let [[entity' sets] (extract-sets entity)]
    (rules plugins entity' sets)))

(defn history-rule
  ([{:keys [plugins] :as entomic} entity & [attr value]]
     (let [[entity' sets] (extract-sets entity)
           attr' (or attr '_)
           value' (or value '_)
           tx-clause [['?entity attr' value' '?transaction]]]
       (history-rules plugins entity' sets tx-clause))))
