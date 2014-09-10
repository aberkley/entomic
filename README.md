#Entomic 

Clojure library for datomic, allows you to express queries as entities.

Warning - library is in the early stages of alpha!

[![Build Status](https://travis-ci.org/aberkley/entomic.svg?branch=master)](https://travis-ci.org/aberkley/entomic)

You can see the latest version on [clojars] (https://clojars.org/org.clojars.aberkley/entomic)

## Motivation

Suppose you create a datom in your application that you would like to persist in datomic. Suppose it looks like this:

```clj
{:book/title "Dune"
:book/author "Frank Herbert"
:book/format "Paperback"}
```

You may wish to know whether it already exists in the database. So you would need to create a query that looks something like this:

```clj
'[:find ?book
  :where
  [?book :book/title "Dune"]
  [?book :book/author "Frank Herbert"
  [?book :book/format "Paperback"]]
```  
  
This is essentially what entomic does - takes an entity and converts it into a datomic query. To find the entity above in the database, you would simply apply the Entomic function f (for "find") to the datom. You may get more than one result, depending on how many entities have those values.

```clj
(f {:book/title "Dune"
    :book/author "Frank Herbert"
    :book/format "Paperback"})
```

More generally, Entomic allows you to express queries as entities, which has several advantages:

* Entities and queries have the same representation so can be used interchangably.
* Queries can be created and manipulated in a more Clojure-idiomatic way - they're just Clojure maps
* Querying is generally more concise

## The Entomic API

This is in the early stages of development but already has a few useful features for querying and committing data to Datomic. See the [tutorial] (https://github.com/aberkley/entomic-tutorial/blob/master/src/entomic_tutorial/core.clj) for a quick tour.



