(ns entomic.test
  (:require [clojure.test :refer :all]
            [datomic.api :only [q db] :as d]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.format :as fmt]
            [entomic.core :as e]
            [entomic.format :as f]
            [entomic.api :as a]))

(def uri "datomic:mem://test")

;;(def uri "datomic:free://localhost:4334/test")

(defprotocol User
  (user-of [this]))

(extend-protocol User
  java.lang.String
  (user-of [p-name]
    (:db/id
     (a/fu {:user/name p-name})))
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
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :book/publishing-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :book/isbn
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :book/rating
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id (d/tempid :db.part/db)
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
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
    :book/author "Iain M. Banks"}
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

(defn example-plugin [? form]
    (let [s (seq form)]
      (if (= 'like (first s))
        `[[(= ~? ~(second s))]])))

(defn init-db! []
  (e/resolve-api! (find-ns 'datomic.api))
  (d/delete-database uri)
  (d/create-database uri)
  (e/set-connection! uri)
  (e/register-plugin! example-plugin)
  (f/set-custom-parser! [:collection/user] user-of)
  (f/set-custom-unparser! [:collection/user] :user/name)
  (e/transact e/conn schema-tx)
  (e/transact e/conn data-tx))

(deftest test-query
  (is (boolean (init-db!)))
  (is (= "Dune" (:book/title (a/fu {:book/title '(like "Dune")}))))
  (is (= "Dune" (:book/title (a/fu {:book/title '(fulltext "Dune")}))))
  (is (= "Excession" (:book/title (a/fu {:book/isbn "9876543210"}))))
  (is (= nil         (:book/title (a/fu {:book/isbn "987654321"}))))
  (is (= "Dune"      (:book/title (a/fu {:book/rating '(> 9M)}))))
  (is (= "Excession" (:book/title (a/fu {:book/rating '[(> 6M) (< 9M)]}))))
  (is (= nil         (:book/title (a/fu {:book/rating '[(> 6M) (< 7M)]}))))
  (is (= "Excession" (:book/title (a/fu {'(bigdec :book/isbn) 9876543210M}))))
  (is (= "Excession" (:book/title (a/fu {'(bigdec :book/isbn) '(> 1234567890)}))))
  (is (= "Excession" (:book/title
                      (:collection/book
                       (a/fu {:collection/user {:user/name "Alex"}
                              :collection/book {:book/title "Excession"}})))))
  (is (= "Dune"      (:book/title (a/fu
                                   {:book/title #{"Dune" "Excession"}
                                    :book/rating '(> 4M)
                                    :book/author #{"Frank Herbert" "Tom Smith"}}))))
  (is (= java.lang.Long (type (a/id {:book/isbn "9876543210"}))))
  (is (boolean (seq (a/ids {:book/isbn "9876543210"}))))
  (is (a/f? {:book/isbn "9876543210"}))
  (is (a/fu? {:book/isbn "9876543210"}))
  (is (= 3 (count (a/f :book))))
  (is (boolean (a/update! [{:book/title "Dune" :book/isbn "9999999999"}] [:book/title])))
  (is (= 1 (count (a/ids {:book/title "Dune"}))))
  (is (a/fu? {:book/isbn "9999999999"}))
  (is (boolean (a/update! [{:book/title "Dune" :book/isbn "1111111111" :book/publishing-date (c/to-date
                                                                                              (t/date-time 2002 4 12))}])))
  (is (= 2 (count (a/ids {:book/title "Dune"}))))
  (is (boolean (a/save! [{:book/title "Excession" :book/isbn "2222222222"}] [:book/title])))
  (is (boolean (a/save! [{:book/title "Excession"
                          :book/author "Iain M. Banks"
                          :book/publishing-date (c/to-date
                                                 (t/date-time 2003 5 28))
                          :book/isbn "9876543210"
                          :book/rating 8.2M}])))
  (is (= 1 (count (a/ids {:book/title "Excession"}))))
  (is (boolean (a/retract! [{:book/title "Excession"
                             :book/rating 8.2M}]
                           [:book/title]
                           :book/rating)))
  (is (nil? (:book/rating (a/fu {:book/title "Excession"}))))
  (is (boolean (a/retract-entities! [{:book/title "Excession"}] [:book/title])))
  (is (nil? (a/fu {:book/title "Excession"})))
  (is (boolean (a/retract-entities! [{:book/title "Dune"
                                      :book/author "Frank Herbert"}])))
  (is (nil? (a/fu {:book/title "Dune"
                   :book/author "Frank Herbert"})))
  (is (= java.lang.Long
         (type
          (:collection/user
           (f/resolve-entity
            {:collection/user {:user/name "Alex"}
             :collection/book {:book/title "Matter"}})))))
  (is (= java.math.BigDecimal
         (type
          (:book/rating
           (f/parse-entity
            {:book/rating "9.5"
             :db/id 17592186045420})))))
  (is (= java.lang.String
         (type
          (:book/isbn
           (f/parse-entity
            {:book/isbn 1234567890})))))
  (is (= java.util.Date
         (type
          (:book/publishing-date
           (f/parse-entity
            {:book/publishing-date (clj-time.core/date-time 2014 1 1)})))))
  (is (= org.joda.time.DateTime
         (type
          (:book/publishing-date
           (f/unparse-entity {:book/publishing-date (c/to-date (t/date-time 2014 1 1))})))))
  (is (boolean (f/set-custom-unparser! [:book/publishing-date] (partial fmt/unparse (fmt/formatters :date)))))
  (is (string?
       (:book/publishing-date
        (f/unparse-entity {:book/publishing-date (c/to-date (t/date-time 2014 1 1))}))))
  (is (nil? (f/unparse-entity nil)))
  (is (= java.lang.Long
         (type
          (:collection/user
           (f/parse-entity {:collection/user "Alex"})))))
  (is (boolean (a/save! [{:book/title "The Player Of Games"
                          :book/author "Iain M. Banks"}])))
  (is (boolean (a/save! [{:collection/user "Alex"
                          :collection/book {:book/title "The Player Of Games"}}])))
  (is (= 3 (count (a/f {:collection/user "Alex"}))))
  (is (boolean (a/save! [{:user/name "Book Club" :user/type :user.type/charity}])))
  (is (boolean (a/fu {:user/type :user.type/charity})))
  (is (boolean (a/save! [{:book/title "Neuromancer" :book/author "William Gibson" :book/isbn "1122334455"}])))
  (is (boolean
       (a/as-transaction!
        [:update [{:book/title "Neuromancer" :book/rating "9.5"}] [:book/title]]
        [:retract [{:book/title "Neuromancer" :book/isbn "1122334455"}] [:book/title] :book/isbn]
        [:save [{:book/title "The Algebraist" :book/author "Iain M. Banks"}]])))
  (is (boolean (a/fu {:book/title "Neuromancer"})))
  (is (= 9.5M (:book/rating (a/fu {:book/title "Neuromancer"}))))
  (is (boolean (seq (a/f {:book/title '?}))))
  (is (nil? (:book/isbn (a/fu {:book/title "Neuromancer"}))))
  (is (boolean (a/fu {:user/dob (t/date-time 1981 10 14)})))
  (is (= "Alex" (:collection/user (a/fu {:collection/user "Alex" :collection/book {:book/title "The Player Of Games"}})))))

(comment

  (where-values '? "Dune")
  (where-values '? '(like "Dune"))
  (a/f {:book/title "Dune"})


  (example-plugin '? '(like "blah"))

  (where-values '? "blah")
  (where-values '? '(likey "blah"))
  )
