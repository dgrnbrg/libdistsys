(ns distrlib.pn-counter-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer (defspec)]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [distrlib.pn-counter :refer :all]))

(defn deltas-to-counter
  ([deltas]
   (deltas-to-counter deltas :node))
  ([deltas node]
   (reduce #(change %1 node %2) (pn-counter) deltas)))

(def is-a-counter
  (prop/for-all
    [deltas (gen/vector gen/int)]
    (= (apply + deltas)
       (value (deltas-to-counter deltas)))))

(defspec check-is-a-counter
  10000
  is-a-counter)

(def combinable
  (prop/for-all
    [a-deltas (gen/vector gen/int)
     b-deltas (gen/vector gen/int)
     c-deltas (gen/vector gen/int)]
    (= (apply + (concat a-deltas b-deltas c-deltas))
       (value (combine
                (deltas-to-counter a-deltas :a)
                (deltas-to-counter b-deltas :b)
                (deltas-to-counter c-deltas :c))))))

(defspec check-is-combinable
  10000
  combinable)

(defn non-zero
  "Restricts a generator to never return zero"
  [gen]
  (gen/such-that (comp not zero?) gen))

(def pos-when-only-positive
  (prop/for-all
    [deltas (gen/not-empty (gen/vector (non-zero gen/pos-int)))]
    (pos? (value (deltas-to-counter deltas)))))

(defspec check-pos-when-only-positive
  1000
  pos-when-only-positive)

(def neg-when-only-negative
  (prop/for-all
    [deltas (gen/not-empty (gen/vector (non-zero gen/neg-int)))]
    (neg? (value (deltas-to-counter deltas))))) 

(defspec check-neg-when-only-negative
  1000
  neg-when-only-negative)
