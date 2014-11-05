(ns distrlib.orswot
  (:refer-clojure :exclude (resolve))
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

(def ^:dynamic *local-counter*)

(def ^:dynamic *dot* (->Dot :n1 0))

(def ^:dynamic *dot-source*)

(defn current-dot
  []
  (cond (bound? #'*dot*) *dot*
        (bound? #'*dot-source*) (*dot-source*)
        :else (throw (ex-info "No dot in dynamic binding!" {}))))

(declare orswot-get orswot-conj orswot-disj)

;; version is a map from node ID to local version
;; data is a map from keys to Value pairs
(deftype Orswot [version data metadata]
  clojure.lang.IPersistentSet
  (disjoin [this o]
    (orswot-disj this (current-dot) o))
  (get [this o]
    (println "in get")
    (orswot-get this o))
  (contains [this o]
    (contains? (.data this) o))

  clojure.lang.IPersistentCollection
  (equiv [this o]
    (and (instance? o Orswot)
         (= (.version o) version)
         (= (.data o) data)))
  (cons [this o]
    (println "hello" this o)
    (orswot-conj this (current-dot) o))
  (empty [this]
    (Orswot. version {} metadata))

  clojure.lang.Counted
  (count [this]
    (count (.data this)))

  clojure.lang.Seqable
  (seq [this]
    (keys data))
  )
;;(class (orswot-conj (orswot) (current-dot) :foo))
;;(.cons (sort (map :name (:members (clojure.reflect/reflect (orswot))))) :foo)
;
;;(orswot-conj (orswot) (current-dot) :fee)
;
;;(.cons (orswot) :fee)
;
(defn orswot
  []
  (Orswot. {} {} nil))

(defn orswot-get
  ([orswot k]
   (orswot-get orswot k nil))
  ([orswot k default]
   (println "in orswot-get")
   (clojure.core/get #spy/d (.data orswot) k default)))

(defn orswot-conj
  "Takes an orswot, a node, a local-version, and list of key value pairs"
  ([orswot dot k]
;;     (assert (< (get-in orswot [:version node] -1) local-version))
   (let [version' (clojure.core/assoc (.version orswot) (:node dot) (:time dot))
         data' (clojure.core/assoc (.data orswot) k dot)]
     (println "Returning orswot with" version' "and" data')
     (Orswot. version' data' (.metadata orswot))))
  ;;TODO is this arity needed?
  #_([orswot dot k & more]
   (reduce #(orswot-conj %1 dot %2) (assoc orswot-conj dot k) more)))

(defn orswot-disj
  [orswot dot k]
  ;; TODO: make these asserts validate at a higher level
 ;; (assert (< (get-in orswot [:version node] -1) local-version))
  (let [version' (clojure.core/assoc (.version orswot) (:node dot) (:time dot))
        data' (clojure.core/dissoc (.data orswot) k)]
    (Orswot. version' data' (.metadata orswot))))

(extend-type Orswot
  CRDT
  (resolve* [orswot1 orswot2]
    (let [keys-in-left (.data orswot1)
          keys-in-right (.data orswot2)
          only-in-left (remove (partial contains? keys-in-right) (keys keys-in-left))
          only-in-right (remove (partial contains? keys-in-left) (keys keys-in-right))
          kept-left (filter (fn [k]
                              (let [{:keys [node time]} (clojure.core/get keys-in-left k)]
                                (> time (clojure.core/get (.version orswot2) node -1))))
                            only-in-left)
          kept-right (filter (fn [k]
                               (let [{:keys [node time]} (clojure.core/get keys-in-right k)]
                                 (> time (clojure.core/get (.version orswot1) node -1))))
                             only-in-right)
          keys-in-both (filter (partial contains? keys-in-right) (keys keys-in-left))
          merged-version (merge-with max (.version orswot1) (.version orswot2))
          ;;TODO handle value conflicts
          merged-both (clojure.core/merge (select-keys keys-in-left (concat kept-left keys-in-both))
                                          (select-keys keys-in-right kept-right))]
      (Orswot. merged-version merged-both (clojure.core/merge (.metadata orswot1)
                                                  (.metadata orswot2))))))

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

(keys (.data (resolve x1 x2)))
(keys (.data (resolve x1 x2 x3)))

(defn keyset-of-orswot
  [orswot]
  (-> orswot :data keys set))

(def keys-added-remain-one-node
  (prop/for-all [v (gen/vector gen/int)]
                (println "lolol")
                (= (set v)
                   (keyset-of-orswot
                     (doto (reduce (fn [orswot [k i]]
                               (orswot-conj orswot (->Dot :node i) k))
                             (orswot)
                             (map vector v (range)))
                       (-> (.version) (println "v"))
                       (-> (.data) (println "d"))
                       )))))

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

(get (conj (orswot) :foo) :foo)
(comment
  (do
    (tc/quick-check 100 keys-added-remain-one-node)
    (tc/quick-check 100 set-semantics)
    (tc/quick-check 100 merge-of-peers)
    (tc/quick-check 100 merge-commutative))
  )

