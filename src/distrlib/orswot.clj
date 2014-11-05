(ns distrlib.orswot
  (:refer-clojure :exclude (get assoc dissoc merge resolve))
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.set :as set]))

(defprotocol CRDT
  (resolve* [r1 r2] "Merges 2 instances of a CRDT")
  ;;TODO how to handle shared version vectors in composites?
  )

(defn resolve
  "Given CRDTs, it combines them."
  ([] nil)
  ([crdt1] crdt1)
  ([crdt1 crdt2] (resolve* crdt1 crdt2))
  ([crdt1 crdt2 & more]
   (reduce resolve (resolve crdt1 crdt2) more)))

;; local version are ints

;; A dot is a node & a local time for that node.
;; version vectors are collections of dots. some
;; optimized CRDTs, like orswot & dvv use them as well
(defrecord Dot [node time])

;; version is a map from node ID to local version
;; data is a map from keys to Value pairs
(defrecord Orswot [version data])

(defn orswot
  []
  (->Orswot {} {}))

(defn get
  ([orswot k]
   (get k nil))
  ([orswot k default]
   (get-in orswot [:data k] default)))

(defn assoc
  "Takes an orswot, a node, a local-version, and list of key value pairs"
  ([orswot dot k]
;;     (assert (< (get-in orswot [:version node] -1) local-version))
   (let [version' (clojure.core/assoc (:version orswot) (:node dot) (:time dot))
         data' (clojure.core/assoc (:data orswot) k dot)]
     (->Orswot version' data')))
  ([orswot dot k & more]
   (reduce #(assoc %1 dot) (assoc orswot dot k) more)))

(defn dissoc
  [orswot dot k]
  ;; TODO: make these asserts validate at a higher level
 ;; (assert (< (get-in orswot [:version node] -1) local-version))
  (let [version' (clojure.core/assoc (:version orswot) (:node dot) (:time dot))
        data' (clojure.core/dissoc (:data orswot) k)]
    (->Orswot version' data')))

(extend-type Orswot
  CRDT
  (resolve* [orswot1 orswot2]
    (let [keys-in-left (:data orswot1)
          keys-in-right (:data orswot2)
          only-in-left (remove (partial contains? keys-in-right) (keys keys-in-left))
          only-in-right (remove (partial contains? keys-in-left) (keys keys-in-right))
          kept-left (filter (fn [k]
                              (let [{:keys [node time]} (clojure.core/get keys-in-left k)]
                                (> time (get-in orswot2 [:version node] -1))))
                            only-in-left)
          kept-right (filter (fn [k]
                               (let [{:keys [node time]} (clojure.core/get keys-in-right k)]
                                 (> time (get-in orswot1 [:version node] -1))))
                             only-in-right)
          keys-in-both (filter (partial contains? keys-in-right) (keys keys-in-left))
          merged-version (merge-with max (:version orswot1 -1) (:version orswot2 -1))
          ;;TODO handle value conflicts
          merged-both (clojure.core/merge (select-keys keys-in-left (concat kept-left keys-in-both))
                                          (select-keys keys-in-right kept-right))]
      (->Orswot merged-version merged-both))))

(def x1 (-> (orswot)
            (assoc (->Dot :n1 0) "hello")))

(def x2 (-> (orswot)
            (assoc (->Dot :n3 0) "привет")
            (assoc (->Dot :n2 1) "hi")
            ))

(def x3
  (-> (orswot)
      (assoc (->Dot :n2 1) "hi")
      (dissoc (->Dot :n2 2) "hi")
      ))

(keys (:data (resolve x1 x2)))
(keys (:data (resolve x1 x2 x3)))

(defn keyset-of-orswot
  [orswot]
  (-> orswot :data keys set))

(def keys-added-remain-one-node
  (prop/for-all [v (gen/vector gen/int)]
                (= (set v)
                   (keyset-of-orswot
                     (reduce (fn [orswot [k i]]
                               (assoc orswot (->Dot :node i) k))
                             (orswot)
                             (map vector v (range)))))))

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
                                 :add (assoc orswot (->Dot node i) k)
                                 :remove (dissoc orswot (->Dot node i) k)
                                 :noop orswot))
                             (orswot)
                             (map #(clojure.core/assoc %1 :i %2) ops (range))))

(def gen-biased-op (gen/frequency [[5 (gen/return :add)] [1 (gen/return :remove)]]))

(def gen-key (gen/elements (map #(keyword (str "key" %)) (range 50))))

(def the-nodes [:n1 :n2 :n3 :n4 :n5])

(def gen-node (gen/elements the-nodes))

(def set-semantics
  (prop/for-all [ops (gen/vector (gen/hash-map :k gen-key
                                               :op gen-biased-op) 10 1000)]
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

(comment
  (do
    (tc/quick-check 100 keys-added-remain-one-node)
    (tc/quick-check 100 set-semantics)
    (tc/quick-check 100 merge-of-peers)
    (tc/quick-check 100 merge-commutative))
  )

