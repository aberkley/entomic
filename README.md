#entomic 

[![Build Status](https://travis-ci.org/aberkley/entomic.svg?branch=master)](https://travis-ci.org/aberkley/entomic)

Clojure library for datomic, allows you to express queries as entities.

Warning - library is in the early stages of alpha!

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
  
This is essentially what entomic does - takes an entity and converts it into a datomic query. This allows you to express queries as entities and therefore manipulate datomic inputs and outputs in a consistent way. It also allows for more concise syntax - for example, to find the entity above in the database, you would simply write this:

```clj
(f {:book/title "Dune"
    :book/author "Frank Herbert"
    :book/format "Paperback"})
```

I.e. just take your existing datom and apply the entomic function "f" to it.

## The Entomic API

This is in the early stages of development and, so far, only has functions for querying datomic. They all involve the same core code and just vary by whether they decorate the returned entities or check for uniqueness.

```clj
f [entity] ;;returns a seq of decorated entities
fu [entity] ;;returns a decorated entity, throws if not unique
ids [entity] ;;returns the result-set
id [entity] ;;returns the result, throws if not unique
f? [entity] ;;returns whether entities exist
fu? [entity] ;;returns whether a unique entity exists
```

## Entity forms

The example above is fairly limited and doesn't even begin to cover the scope of querying with datomic. In order to cover a wider range of cases, Entomic allows more complex syntax inside of the entities. These can all be used in conjuction with each other and composed.

```clj
(f {:book/isbn-10 "9876543210"})           ;; simple query, same as above
(f {:book/rating '(> 9M)})                 ;; using a predicate on the value
(f {:book/rating '[(> 6M) (< 9M)]})        ;; AND using [] (and predicates as in previous)
(f {'(bigdec :book/isbn) 9876543210M})     ;; using a function on the key
(f {'(bigdec :book/isbn) '(> 1234567890)}) ;; using a function on the key and a predicate together
(f {:collection/user {:user/name "Alex"}}) ;; JOIN using an entity within an entity
(f {:book/title #{"Dune" "Excession"}
    :book/rating '(> 4M)))                 ;; OR using #{} and predicate on a different value
```

## Tutorial

TODO


