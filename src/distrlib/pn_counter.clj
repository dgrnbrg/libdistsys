(ns distrlib.pn-counter
 (:require [distrlib.crdt :as crdt]))

;;; We'll represent a PN Counter as a pair of g-counters, each of those
;;; being represented as a map.

(defn merge-g-counters
  [& gcounters]
  (apply merge-with max gcounters))

(defn read-g-counter
  [gcounter]
  (apply + (vals gcounter)))

(declare combine)

(defrecord PNCounter [incs decs]
  crdt/CRDT
  (type [this] :crdt/pn-counter)
  (resolve* [r1 r2]
    (combine r1 r2)))

(defn pn-counter
  []
  (->PNCounter nil nil))

(defn value
  [pn-counter]
  (- (read-g-counter (:incs pn-counter))
     (read-g-counter (:decs pn-counter))))

(defn change
  [pn-counter node amount]
  (if (neg? amount)
    (update-in pn-counter [:decs node] (fnil - 0) amount)
    (update-in pn-counter [:incs node] (fnil + 0) amount)))

(defn combine
  [& pn-counters]
  (reduce (fn [{ti :incs td :decs} {i :incs d :decs}]
            (->PNCounter (merge-g-counters ti i) (merge-g-counters td d)))
          (pn-counter)
          pn-counters))
