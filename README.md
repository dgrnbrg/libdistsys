[![Build Status](https://travis-ci.org/dgrnbrg/libdistsys.svg?branch=master)](https://travis-ci.org/dgrnbrg/libdistsys)

# libdistsys

A Clojure library for building distributed systems.

Currently, there are 2 working features in libdistsys:

1. Implementations of state-of-the-art CRDTs (convergent replicated datatypes)
1. A deterministic network simulator with support for offline analysis

## CRDTs

CRDTs, or convergent/commutative/conflict-free replicated datatypes, are datatypes which can be edited independently on different machines, but can still be automatically merged to form a consistent view of their state.
This makes them ideal for building highly available distributed systems, since nodes can continue to work during temporary network failures, and when they sync, they'll automatically have semantically valid data.

libdistsys currently supports distributed sets (to which you can add or remove elements) and distributed counters (to which you can add or subtract arbitrary amounts).

### General Usage

When you want to combine 2 or more instances of a CRDT (for instance, because you recieved an updated view of a CRDT over the network and want to reconcile it with your local one), you should use the `distrlib.crdt/resolve` function. For example:

```clojure
(distrlib.crdt/resolve crdt-local crdt-recieved)
```

By default, libdistsys assumes that each JVM process is a unique actor in your system.
If you'd like to have multiple actors in a single JVM, use `distrlib.crdt/with-node` to provide a new UUID to all the code running in the body of the `with-node`.
For example, we can make a new UUID and use that for some CRDT code:

```clojure
(let [my-new-node (java.util.UUID/randomUUID)]
  (distrlib.crdt/with-node my-new-node
    ;; CRDT code goes here
    ))
```

### Distributed Sets (aka Orswots)

libdistsys implements the "orswot" CRDT as its set abstraction.
See the code for details about how it works.
The key reason for loving orswots is that they automatically prune their discarded data, and so their size is only a function of the amount of data they contain (unlike many earlier CRDTs, which grow with the number of operations in their history).

```clojure
(use 'distrlib.orswot)

;; Define a remote node
(def remote-node (java.util.UUID/randomUUID))

;; create an orswot
(def my-set (orswot))

;; Add local and remote values, then merge
;; orswots support many normal Clojure set API functions, like conj, disj, count, seq, and empty
(def local-variant (conj orswot :foo))
(def remote-variant (distrlib.crdt/with-node remote-node (conj orswot :bar)))
(def merged (distrlib.crdt/resolve local-variant remote-variant))

;; We can now remove the key which the remote added from the local variant
;; When we merge the remote in again, since we already removed that key, it stays gone
;; (this is the clever bit)
(def merged-update (disj merged :bar))
(= (seq merged-update) (seq (distrlib.crdt/resolve merged-update remote-variant)))

```

If you would rather not use global dynamic var magic for operations on orswots, then you can use the update functions in `distrlib.orswot`: `orswot-conj` and `orswot-disj`.
These functions take the node as an explicit argument.

### Distributed Counters (aka PN-counters)

libdistsys implements PN counters for its counters.
PN counters are linear in size with the number of actors that have ever affected their count.

```clojure
(use 'distrlib.pn-counter)

;; PN counters don't implicitly pick up the node they're running in; you must always provide it

(def c1 (pn-counter))

;; "value" gets the current counter value
(= (value c1) 0)

(def node1 (java.util.UUID/randomUUID))
(def node2 (java.util.UUID/randomUUID))

;; "change" takes the PN counter, the node, and the amount to add
;; Of course, you can provide a negative number to deduct from the counter
(def c2 (change c1 node1 10))

(def c3 (change c2 node1 -3))

(def d1 (change c1 node2 -1))

(= (value (distrlib.crdt/resolve c3 d1)) 6)
```

## Testing

The above listed features are tested using generative testing to verify their propertie to verify their properties

## License

Copyright © 2014, 2015 David Greenberg

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
