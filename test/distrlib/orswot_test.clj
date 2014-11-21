(ns distrlib.orswot-test
  (:require [clojure.test :refer :all]
            [distrlib.crdt :as crdt]
            [clojure.test.check :as tc]
            [clojure.set :as set]
            [clojure.test.check.clojure-test :refer (defspec)]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [distrlib.orswot :refer :all]))

(def x1 (-> (orswot)
            (clojure.core/conj  "hello")))

(def x2 (-> (orswot)
            (orswot-conj :n3 "привет")
            (orswot-conj :n2 "hi")
            ))

(def x3
  (-> (orswot)
      (orswot-conj :n2 "hi")
      (orswot-disj :n2 "hi")
      ))

(deftest basic-resolving
  (is (= #{"hello" "привет" "hi"} (set (crdt/resolve x1 x2))))
  (is (=  #{"hello" "привет"} (set (crdt/resolve x1 x2 x3)))))

(defn keyset-of-orswot
  [orswot]
  (into #{} orswot))

(def keys-added-remain-one-node
  (prop/for-all [v (gen/vector gen/int)]
                (= (set v)
                   (keyset-of-orswot
                     (reduce (fn [orswot [k i]]
                               (orswot-conj orswot :node k))
                             (orswot)
                             (map vector v (range)))))))

(orswot-conj (orswot) :node :foo)

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
                                 :add (orswot-conj orswot node k)
                                 :remove (orswot-disj orswot node k)
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
                          (apply crdt/resolve))))))

(def merge-commutative
  (prop/for-all [ops (gen/vector (gen/hash-map :node gen-node
                                               :k gen-key
                                               :op gen-biased-op) 10 1000)
                 [node-perm-1 node-perm-2] (gen/such-that (fn [[l r]] (not= l r))
                                                          (gen/tuple (gen/shuffle the-nodes) (gen/shuffle the-nodes)))]
                (let [orswots (->> (concat (map #(hash-map :op :noop :node %) the-nodes)
                                           ops)
                                   (group-by :node)
                                   (map (fn [[node ops]] [node (run-ops-as-orswot node ops)]))
                                   (into {}))]
                  (= (keyset-of-orswot (apply crdt/resolve (map orswots node-perm-1)))
                     (keyset-of-orswot (apply crdt/resolve (map orswots node-perm-2)))))))

(get (orswot-conj (orswot) :node :foo) :foo)

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

(deftest vclock-compare
  (is (crdt/vclock-descends {:a 0 :b 0} {:a 0 :b 0}))
  (is (crdt/vclock-descends {:a 0 :b 0} {}))
  (is (crdt/vclock-descends {:a 3 :b 3} {:a 1}))
  (is (crdt/vclock-descends {:a 3 :b 3} {:a 3}))
  (is (not (crdt/vclock-descends {:a 3 :b 3} {:a 3 :c 1})))
  (is (not (crdt/vclock-descends {:a 3 :b 3} {:a 4 :b 1}))))

(deftest vclock-pruning
  (is (= {} (crdt/vclock-prune {:a 2 :b 2} {:a 3 :b 2})))
  (is (= {} (crdt/vclock-prune {:a 2 :b 2} {:a 3 :b 3})))
  (is (= {:a 3} (crdt/vclock-prune {:a 3 :b 2} {:a 2 :b 3}))))

(deftest opposites-removal
  (let [s (orswot)
        s-a (orswot-conj s :a :foo)
        s-b (orswot-conj s :b :bar)
        s-a' (orswot-disj s-a :b :bar)
        s-b' (orswot-disj s-b :a :foo)
        final (crdt/resolve s-a' s-b')]
    (is (= (set s-a') #{:foo}))
    (is (= (set s-b') #{:bar}))
    (is (= (set final) #{}))))

(deftest stats-test
  (let [s (orswot)
        s1 (orswot-conj s 1 :foo)
        s2 (orswot-conj s1 2 :foo)
        s3 (orswot-conj s2 3 :bar)
        s4 (orswot-disj s3 1 :foo)]
    (is (= {:actor-count 0 :element-count 0 :max-dot-length 0}
           (orswot-stats s)))
    (is (= 3 (:actor-count (orswot-stats s4))))
    (is (= 1 (:element-count (orswot-stats s4))))
    (is (= 1 (:max-dot-length (orswot-stats s4))))))

(deftest disjoint-merge-test
  (let [a1 (orswot-conj (orswot) 1 :bar)
        b1 (orswot-conj (orswot) 2 :baz)
        c (crdt/resolve a1 b1)
        a2 (orswot-disj a1 1 :bar)
        d (crdt/resolve a2 c)]
    (is (= #{:baz} (set d)))))

(deftest present-but-removed-test
  (let [a (orswot-conj (orswot) :a :Z)
        c a
        a2 (orswot-disj a :a :Z)
        b (orswot-conj (orswot) :b :Z)
        a3 (crdt/resolve b a2)
        b2 (orswot-disj b :b :Z)
        merged (crdt/resolve a3 c b2)]
    (is (= (set merged) #{}))))

(deftest no-dots-left-test
  (let [a (orswot-conj (orswot) :a :Z)
        b (orswot-conj (orswot) :b :Z)
        c a
        a2 (orswot-disj a :a :Z)
        a3 (crdt/resolve a2 b)
        b2 (orswot-disj b :b :Z)
        b3 (crdt/resolve b2 c)
        merged (crdt/resolve a3 b3 c)]
    (is (= #{} (set merged)))))
