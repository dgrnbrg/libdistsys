(ns distrlib.orswot
  (:refer-clojure :exclude (get assoc dissoc merge))
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.set :as set]))

;; local version are ints

;; This is the value of a key + the node & version it was stored at
;; node & local version form the dot
(defrecord Value [node local-version value])

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
  ([orswot node local-version k]
   (assoc orswot node local-version k nil))
  ([orswot node local-version k v]
;;     (assert (< (get-in orswot [:version node] -1) local-version))
   (let [version' (clojure.core/assoc (:version orswot) node local-version)
         data' (clojure.core/assoc (:data orswot) k (->Value node local-version v))]
     (->Orswot version' data')))
  ([orswot node local-version k v & kvs]
   ;(assoc orswot node local-version k v)
   )
  )

(defn dissoc
  [orswot node local-version k]
  ;; TODO: make these asserts validate at a higher level
 ;; (assert (< (get-in orswot [:version node] -1) local-version))
  (let [version' (clojure.core/assoc (:version orswot) node local-version)
        data' (clojure.core/dissoc (:data orswot) k)]
    (->Orswot version' data')))

(defn merge
  ([] (orswot))
  ([orswot] orswot)
  ([orswot1 orswot2]
   (let [keys-in-left (:data orswot1)
         keys-in-right (:data orswot2)
         only-in-left (remove (partial contains? keys-in-right) (keys keys-in-left))
         only-in-right (remove (partial contains? keys-in-left) (keys keys-in-right))
         kept-left (filter (fn [k]
                             (let [{dot-node :node dot-version :local-version} (clojure.core/get keys-in-left k)]
                               (> dot-version (get-in orswot2 [:version dot-node] -1))))
                           only-in-left)
         kept-right (filter (fn [k]
                              (let [{dot-node :node dot-version :local-version} (clojure.core/get keys-in-right k)]
                                (> dot-version (get-in orswot1 [:version dot-node] -1))))
                            only-in-right)
         keys-in-both (filter (partial contains? keys-in-right) (keys keys-in-left))
         merged-version (merge-with max (:version orswot1 -1) (:version orswot2 -1))
         ;;TODO handle value conflicts
         merged-both (clojure.core/merge (select-keys keys-in-left (concat kept-left keys-in-both))
                                         (select-keys keys-in-right kept-right))]
     (->Orswot merged-version merged-both)))
  ([o1 o2 & os]
   (reduce merge (list* o1 o2 os))))

(def x1 (-> (orswot)
            (assoc :n1 0 "hello")))

(def x2 (-> (orswot)
            (assoc :n3 0 "привет")
            (assoc :n2 1 "hi")
            ))

(def x3
  (-> (orswot)
      (assoc :n2 1 "hi")
      (dissoc :n2 2 "hi")
      ))

(keys (:data (merge x1 x2)))
(keys (:data (merge x1 x2 x3)))

(defn keyset-of-orswot
  [orswot]
  (-> orswot :data keys set))

(def keys-added-remain-one-node
  (prop/for-all [v (gen/vector gen/int)]
                (= (set v)
                   (keyset-of-orswot
                     (reduce (fn [orswot [k i]]
                               (assoc orswot :node i k))
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
                                 :add (assoc orswot node i k)
                                 :remove (dissoc orswot node i k)
                                 :noop orswot))
                             (orswot)
                             (map #(clojure.core/assoc %1 :i %2) ops (range))))

(def set-semantics
  (prop/for-all [ops (gen/vector (gen/hash-map :k gen/int
                                               :op (gen/elements [:add :remove])) 10 1000)]
                (= (run-ops-as-set ops)
                   (keyset-of-orswot
                     (run-ops-as-orswot :node ops)))))

(def gen-biased-op (gen/frequency [[5 (gen/return :add)] [1 (gen/return :remove)]]))

(def the-nodes [:n1 :n2 :n3 :n4 :n5])

(def gen-node (gen/elements the-nodes))

(def merge-of-peers
  (prop/for-all [ops (gen/vector (gen/hash-map :node gen-node
                                               :k gen/int
                                               :op gen-biased-op) 10 1000)]
                (= (->> (group-by :node ops)
                        vals
                        (map run-ops-as-set)
                        (apply set/union))
                   (keyset-of-orswot
                     (->> (group-by :node ops)
                          (map (fn [[node ops]] (run-ops-as-orswot node ops)))
                          (apply merge))))))

(defn prefixes
  [s]
  (map #(take (inc %) s) (range (count s))))

(def merge-commutative
  (prop/for-all [ops (gen/vector (gen/hash-map :node gen-node
                                               :k gen/int
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
                  (= (keyset-of-orswot (apply merge orswots))
                     (keyset-of-orswot (apply merge permuted))))))

(comment
  (tc/quick-check 100 keys-added-remain)
  (tc/quick-check 100 set-semantics)
  (tc/quick-check 100 merge-of-peers)
  (tc/quick-check 100 merge-commutative)
  )

