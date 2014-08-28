(ns entomic.core-test
  (:use     [entomic.core :as e])
  (:require [clojure.test :refer :all]
            [datomic.api :only [q db] :as d]
            [clj-time.core :as t]
            [clj-time.coerce :as c]))

(e/resolve-api! (find-ns 'datomic.api))

(defonce uri "datomic:mem://test")

(d/delete-database uri)

(d/create-database uri)

(def conn' (d/connect uri))

(e/set-connection! conn')

(d/transact conn'
 [{:db/id (d/tempid :db.part/db)
   :db/ident :book/title
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db.install/_attribute :db.part/db}
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
   :db/ident :collection/user
   :db/valueType :db.type/ref
   :db/cardinality :db.cardinality/one
   :db.install/_attribute :db.part/db}
  {:db/id (d/tempid :db.part/db)
   :db/ident :collection/book
   :db/valueType :db.type/ref
   :db/cardinality :db.cardinality/one
   :db.install/_attribute :db.part/db}])

(d/transact conn'
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
             {:db/id (d/tempid :db.part/user -3)
              :user/name "Alex"
              :user/dob (c/to-date
                         (t/date-time 1981 10 14))}
             {:db/id (d/tempid :db.part/user -4)
              :collection/user (d/tempid :db.part/user -3)
              :collection/book (d/tempid :db.part/user -2)}])

(deftest query
  (is (= "Excession" (:book/title (e/fu {:book/isbn "9876543210"}))))
  (is (= nil         (:book/title (e/fu {:book/isbn "987654321"}))))
  (is (= "Dune"      (:book/title (e/fu {:book/rating '(> 9M)}))))
  (is (= "Excession" (:book/title (e/fu {:book/rating '[(> 6M) (< 9M)]}))))
  (is (= nil         (:book/title (e/fu {:book/rating '[(> 6M) (< 7M)]}))))
  (is (= "Excession" (:book/title (e/fu {'(bigdec :book/isbn) 9876543210M}))))
  (is (= "Excession" (:book/title (e/fu {'(bigdec :book/isbn) '(> 1234567890)}))))
  (is (= "Excession" (:book/title (:collection/book
                                   (e/fu {:collection/user {:user/name "Alex"}})))))
  (is (= "Dune"      (:book/title (e/fu
                                   {:book/title #{"Dune" "Excession"}
                                    :book/rating '(> 4M)
                                    :book/author #{"Frank Herbert" "Tom Smith"}}))))
  (is (boolean (e/id {:book/isbn "9876543210"})))
  (is (boolean (seq (ids {:book/isbn "9876543210"}))))
  (is (e/f? {:book/isbn "9876543210"}))
  (is (e/fu? {:book/isbn "9876543210"})))

(comment
  (e/f? {:book/title #{"Dune" "Excession"}
         :book/rating '(> 4M)
         :book/author #{"Frank Herbert" "Tom Smith"}})
  )
