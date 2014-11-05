(ns distrlib.orswot-test
  (:refer-clojure :exclude (resolve))
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.set :as set]
            [clojure.test.check.clojure-test :refer (defspec)]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [distrlib.orswot :refer :all]))

(def x1 (-> (orswot)
            (clojure.core/conj  "hello")))

(def x2 (-> (orswot)
            (orswot-conj (->Dot :n3 0) "привет")
            (orswot-conj (->Dot :n2 1) "hi")
            ))

(def x3
  (-> (orswot)
      (orswot-conj (->Dot :n2 1) "hi")
      (orswot-disj (->Dot :n2 2) "hi")
      ))

(deftest basic-resolving
  (is (= #{"hello" "привет" "hi"} (set (resolve x1 x2))))
  (is (=  #{"hello" "привет"} (set (resolve x1 x2 x3)))))

(defn keyset-of-orswot
  [orswot]
  (into #{} orswot))

(def keys-added-remain-one-node
  (prop/for-all [v (gen/vector gen/int)]
                (= (set v)
                   (keyset-of-orswot
                     (reduce (fn [orswot [k i]]
                               (orswot-conj orswot (->Dot :node i) k))
                             (orswot)
                             (map vector v (range)))))))

(orswot-conj (orswot) (->Dot :node 0) :foo)

(defn run-ops-as-set
  [ops]
  (reduce (fn [s {:keys [k op]}]
            (case op
              :add (conj s k)
              :remove (disj s k)
              :noop s))
          #{}
          ops))

(defn run-ops-as-orswot
  [node ops]
  (reduce (fn [orswot {:keys [k op i]}]
                               (case op
                                 :add (orswot-conj orswot (->Dot node i) k)
                                 :remove (orswot-disj orswot (->Dot node i) k)
                                 :noop orswot))
                             (orswot)
                             (map #(clojure.core/assoc %1 :i %2) ops (range))))

(def gen-biased-op (gen/frequency [[5 (gen/return :add)] [1 (gen/return :remove)]]))

(def gen-key (gen/elements (map #(keyword (str "key" %)) (range 50))))

(def the-nodes [:n1 :n2 :n3 :n4 :n5])

(def gen-node (gen/elements the-nodes))

(def set-semantics
  (prop/for-all [ops (gen/vector (gen/hash-map :k gen-key
                                               :op gen-biased-op) 10 100)]
                (= (run-ops-as-set ops)
                   (keyset-of-orswot
                     (run-ops-as-orswot :node ops)))))

(def merge-of-peers
  (prop/for-all [ops (gen/vector (gen/hash-map :node gen-node
                                               :k gen-key
                                               :op gen-biased-op) 10 1000)]
                (= (->> (group-by :node ops)
                        vals
                        (map run-ops-as-set)
                        (apply set/union))
                   (keyset-of-orswot
                     (->> (group-by :node ops)
                          (map (fn [[node ops]] (run-ops-as-orswot node ops)))
                          (apply resolve))))))

(defn prefixes
  [s]
  (map #(take (inc %) s) (range (count s))))

(def merge-commutative
  (prop/for-all [ops (gen/vector (gen/hash-map :node gen-node
                                               :k gen-key
                                               :op gen-biased-op) 10 1000)
                 knuth-suffle (apply gen/tuple (map gen/elements (prefixes (range (count the-nodes)))))]
                (let [orswots (->> (concat (map #(hash-map :op :noop :node %) the-nodes)
                                           ops)
                                   (group-by :node)
                                   (map (fn [[node ops]] (run-ops-as-orswot node ops))))
                      permuted (reduce (fn [perm [i j]]
                                         (clojure.core/assoc perm
                                                             i (nth perm j)
                                                             j (nth perm i)))
                                       (vec orswots)
                                       (map vector (range) knuth-suffle))]
                  (= (keyset-of-orswot (apply resolve orswots))
                     (keyset-of-orswot (apply resolve permuted))))))

(get (orswot-conj (orswot) (->Dot :node 0) :foo) :foo)

(defspec check-basic-add-keys
  1000
  keys-added-remain-one-node)

(defspec check-single-node-matches-set
  1000
  set-semantics)

(defspec check-resolve-does-merge-when-independent
  1000
  merge-of-peers)

(defspec check-merge-is-commutative
  1000
  merge-commutative)
