(ns entomic.test
  (:require [clojure.test :refer :all]
            [datomic.api :only [q db] :as d]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.format :as fmt]
            [entomic.core :as e]
            [entomic.format :as f]
            [entomic.api :as a]
            [entomic.system :as sys]))

(def uri "datomic:mem://test")

(defprotocol User
  (user-of [this]))

(extend-protocol User
  java.lang.String
  (user-of [p-name]
    {:user/name p-name})
  java.lang.Object
  (user-of [this]
    this))

(def schema-tx
  [{:db/id (d/tempid :db.part/db)
    :db/ident :book/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/fulltext true}
   {:db/id (d/tempid :db.part/db)
    :db/ident :book/author
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/fulltext true}
   {:db/id (d/tempid :db.part/db)
    :db/ident :book/publishing-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :book/isbn
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/fulltext true}
   {:db/id (d/tempid :db.part/db)
    :db/ident :book/rating
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db
    :db/fulltext true}
   {:db/id (d/tempid :db.part/db)
    :db/ident :user/dob
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :user/type
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   [:db/add #db/id[:db.part/user] :db/ident :user.type/charity]
   {:db/id (d/tempid :db.part/db)
    :db/ident :collection/user
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :collection/book
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def data-tx
  [{:db/id (d/tempid :db.part/user -1)
    :book/title "Dune"
    :book/author "Frank Herbert"
    :book/publishing-date (c/to-date
                           (t/date-time 1971 6 12))
    :book/isbn "1234567890"
    :book/rating 9.5M}
   {:db/id (d/tempid :db.part/user -2)
    :book/title "Excession"
    :book/author "Iain M. Banks"
    :book/publishing-date (c/to-date
                           (t/date-time 2003 5 28))
    :book/isbn "9876543210"
    :book/rating 8.2M}
   {:db/id (d/tempid :db.part/user -6)
    :book/title "Matter"
    :book/isbn "0000000000"
    :book/author "Iain M. Banks"}
   {:db/id (d/tempid :db.part/user -7)
    :book/title "From Hell"
    :book/isbn "0000000001"
    :book/author "Alan Moore"}
   {:db/id (d/tempid :db.part/user -3)
    :user/name "Alex"
    :user/dob (c/to-date
               (t/date-time 1981 10 14))}
   {:db/id (d/tempid :db.part/user -4)
    :collection/user (d/tempid :db.part/user -3)
    :collection/book (d/tempid :db.part/user -2)}
   {:db/id (d/tempid :db.part/user -5)
    :collection/user (d/tempid :db.part/user -3)
    :collection/book (d/tempid :db.part/user -6)}])

(defn save-sequels! [n title author]
  (->> n
       (range 2)
       (map (fn [n'] {:book/title (str title " " n')
                     :book/author author}))
       a/save!))

(defn example-plugin [? form]
    (let [s (seq form)]
      (if (= 'like (first s))
        `[[(= ~? ~(second s))]])))

(def entomic (atom nil))

(defn new-test-system []
  (d/delete-database uri)
  (d/create-database uri)
  (d/transact (d/connect uri) schema-tx)
  (d/transact (d/connect uri) data-tx)
  (.start
   (sys/new-entomic uri
                    (find-ns 'datomic.api)
                    {:parsers {:collection/user user-of}
                     :unparsers {:collection/user :user/name
                                 :user/dob (partial fmt/unparse (fmt/formatters :date))}}
                    [example-plugin])))

(def entomic (-> (new-test-system)
                 .start
                 :entomic))

(deftest test-query
  (is (= "Excession" (:book/title (a/fu entomic {:book/isbn "9876543210"}))))
  (is (= "Dune" (:book/title (a/fu entomic {:book/title '(like "Dune")}))))
  (is (= "Dune" (:book/title (a/fu entomic {:book/title '(fulltext "Dune")}))))
  (is (= nil         (:book/title (a/fu entomic {:book/isbn "987654321"}))))
  (is (= "Dune"      (:book/title (a/fu entomic {:book/rating '(> 9M)}))))
  (is (= "Excession" (:book/title (a/fu entomic {:book/rating '[(> 6M) (< 9M)]}))))
  (is (= nil         (:book/title (a/fu entomic {:book/rating '[(> 6M) (< 7M)]}))))
  (is (= "Excession" (:book/title (a/fu entomic {'(bigdec :book/isbn) 9876543210M}))))
  (is (= "Excession" (:book/title (a/fu entomic {'(bigdec :book/isbn) '(> 1234567890)}))))
  (is (= "Excession" (:book/title
                      (:collection/book
                       (a/fu entomic {:collection/user {:user/name "Alex"}
                                      :collection/book {:book/title "Excession"}})))))
  (is (= 1 (count (a/f entomic {:collection/user {:user/name #{"Alex" "Bill"}}
                             :collection/book {:book/title #{"Matter" "Excession"}
                                               :book/isbn "0000000000"}}))))
  (is (= "Dune"      (:book/title (a/fu entomic
                                   {:book/title #{"Dune" "Excession"}
                                    :book/rating '(> 4M)
                                    :book/author #{"Frank Herbert" "Tom Smith"}}))))
  (is (= java.lang.Long (type (a/id entomic {:book/isbn "9876543210"}))))
  (is (boolean (seq (a/ids entomic {:book/isbn "9876543210"}))))
  (is (a/f? entomic {:book/isbn "9876543210"}))
  (is (a/fu? entomic {:book/isbn "9876543210"}))
  (is (a/fu? entomic #{{:book/title "Dune"}
                       {:book/author "Frank Herbert"}}))
  (is (= 4 (count (a/f entomic :book))))
  (is (boolean (a/update! entomic [{:book/title "Dune" :book/isbn "9999999999"}] [:book/title])))
  (is (= 1 (count (a/ids entomic {:book/title "Dune"}))))
  (is (a/fu? entomic {:book/isbn "9999999999"}))
  (is (boolean (a/update! entomic [{:book/title "Dune" :book/isbn "1111111111" :book/publishing-date (c/to-date
                                                                                              (t/date-time 2002 4 12))}])))
  (is (= 2 (count (a/ids entomic {:book/title "Dune"}))))
  (is (boolean (a/save! entomic [{:book/title "Excession" :book/isbn "2222222222"}] [:book/title])))
  (is (boolean (a/save! entomic [{:book/title "Excession"
                          :book/author "Iain M. Banks"
                          :book/publishing-date (c/to-date
                                                 (t/date-time 2003 5 28))
                          :book/isbn "9876543210"
                          :book/rating 8.2M}])))
  (is (= 1 (count (a/ids entomic {:book/title "Excession"}))))
  (is (boolean (a/retract! entomic [{:book/title "Excession"
                             :book/rating 8.2M}]
                           [:book/title]
                           :book/rating)))
  (is (nil? (:book/rating (a/fu entomic {:book/title "Excession"}))))
  (is (boolean (a/retract-entities! entomic [{:book/title "Excession"}] [:book/title])))
  (is (nil? (a/fu entomic {:book/title "Excession"})))
  (is (boolean (a/retract-entities! entomic [{:book/title "Dune"
                                      :book/author "Frank Herbert"}])))
  (is (nil? (a/fu entomic {:book/title "Dune"
                   :book/author "Frank Herbert"})))
  (is (= java.lang.Long
         (type
          (:collection/user
           (f/resolve-entity entomic
            {:collection/user {:user/name "Alex"}
             :collection/book {:book/title "Matter"}})))))
  (is (= java.math.BigDecimal
         (type
          (:book/rating
           (f/parse-entity entomic
            {:book/rating "9.5"
             :db/id 17592186045420})))))
  (is (= java.lang.String
         (type
          (:book/isbn
           (f/parse-entity entomic
            {:book/isbn 1234567890})))))
  (is (= java.util.Date
         (type
          (:book/publishing-date
           (f/parse-entity entomic
            {:book/publishing-date (t/date-time 2014 1 1)})))))
  (is (= org.joda.time.DateTime
         (type
          (:book/publishing-date
           (f/unparse-entity entomic {:book/publishing-date (c/to-date (t/date-time 2014 1 1))})))))
  (is (= java.lang.String
         (type
          (:user/dob
           (f/unparse-entity entomic {:user/dob (c/to-date (t/date-time 2014 1 1))})))))
  (is (nil? (f/unparse-entity entomic nil)))
  (is (= java.lang.Long
         (type
          (:collection/user
           (f/resolve-entity entomic {:collection/user "Alex"})))))
  (is (boolean (a/save! entomic [{:book/title "The Player Of Games"
                          :book/author "Iain M. Banks"}])))
  (is (boolean (a/save! entomic [{:collection/user "Alex"
                                  :collection/book {:book/title "The Player Of Games"}}])))
  (is (= 3 (count (a/f entomic {:collection/user "Alex"}))))
  (is (boolean (a/save! entomic [{:user/name "Book Club" :user/type :user.type/charity}])))
  (is (boolean (a/fu entomic {:user/type :user.type/charity})))
  (is (boolean (a/save! entomic [{:book/title "Neuromancer" :book/author "William Gibson" :book/isbn "1122334455"}])))
  (is (boolean
       (a/as-transaction! entomic
        [:update [{:book/title "Neuromancer" :book/rating "9.5"}] [:book/title]]
        [:retract [{:book/title "Neuromancer" :book/isbn "1122334455"}] [:book/title] :book/isbn]
        [:save [{:book/title "The Algebraist" :book/author "Iain M. Banks"}]])))
  (is (boolean (a/fu entomic {:book/title "Neuromancer"})))
  (is (= 9.5M (:book/rating (a/fu entomic {:book/title "Neuromancer"}))))
  (is (boolean (seq (a/f entomic {:book/title '?}))))
  (is (nil? (:book/isbn (a/fu entomic {:book/title "Neuromancer"}))))
  (is (boolean (a/fu entomic {:user/dob (t/date-time 1981 10 14)})))
  (is (= "Alex" (:collection/user (a/fu entomic {:collection/user "Alex"
                                                 :collection/book {:book/title "The Player Of Games"}})))))

(defn create-history! []
  (a/save! entomic [{:book/title "1984" :book/author "George Orwell"}])
  (a/update! entomic [{:book/title "1984" :book/isbn "1"}] [:book/title])
  (a/update! entomic [{:book/title "1984" :book/isbn "2"}] [:book/title])
  (a/update! entomic [{:book/title "1984" :book/isbn "3"}] [:book/title])
  (a/update! entomic [{:book/title "1984" :book/author "G. Orwell"}] [:book/title]))

(deftest test-history
  (create-history!)
  (is (= "3" (:book/isbn (a/fu entomic {:book/title "1984"}))))
  (is (= 5 (count (a/h entomic {:book/author "George Orwell"})))))
