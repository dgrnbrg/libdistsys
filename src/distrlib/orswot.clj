(ns distrlib.orswot
  (:require [distrlib.crdt :as crdt]))

;; Ideas:
;; - Update operations should only take the node, not the time, because the time
;; is implied by the version vector of the update (maybe not true; try to prove)
;; - Version vector can live outside any specific CRDT for efficiency/sharing in
;; combined structures (could be hard b/c orswot leverages this heavily?)
;; - share code between DVV, orswot, and ormwot

;; Add support for context-based removes; requires support deferred opertations

;; A dot is a node & a local time for that node.
;; version vectors are collections of dots. some
;; optimized CRDTs, like orswot & dvv use them as well

(declare orswot-get orswot-conj orswot-disj orswot-resolve)

;;;TODO have test that generates in addition to commands gossips to do each cycle
;;;     then we can check properties while it's partially replicated

;; need to store `disj` ops until the orswot's version is past the disj

;; version is a map from node ID to local version
;; data is a map from keys to Value pairs
(deftype Orswot [version data metadata]
  clojure.lang.IPersistentSet
  (disjoin [this o]
    (orswot-disj this crdt/*current-node* o))
  (get [this o]
    (orswot-get this o))
  (contains [this o]
    (contains? (.data this) o))

  java.lang.Object
  (equals [this o]
    (or (identical? this o)
        (and (set? o)
             (= o (into #{} this)))))
  (hashCode [this]
    (bit-xor (.hashCode data) (.hashCode version)))

  clojure.lang.IHashEq
  (hasheq [this]
    (bit-xor (hash data) (hash version)))

  clojure.lang.IPersistentCollection
  (equiv [this o]
    (and (instance? Orswot o)
         (= (.version o) version)
         (= (.data o) data)))
  (cons [this o]
    (orswot-conj this crdt/*current-node* o))
  (empty [this]
    (Orswot. (crdt/vclock-inc version crdt/*current-node*) {} metadata))

  clojure.lang.Counted
  (count [this]
    (count (.data this)))

  clojure.lang.Seqable
  (seq [this]
    (keys data))
  
  crdt/CRDT
  (type [this] :crdt/orswot)
  (resolve* [orswot1 orswot2]
    (when-not (instance? Orswot orswot2)
      (throw (ex-info "2nd argument must also be an orswot" {:orswot2 orswot2})))
    (orswot-resolve orswot1 orswot2)))

(defn orswot
  []
  (Orswot. {} {} nil))

(defmethod print-method Orswot [v ^java.io.Writer w]
  (.write w (str "#Orswot{:version " (.version v) ", :data " (.data v) "}")))

(defn orswot-get
  ([orswot k]
   (orswot-get orswot k nil))
  ([orswot k default]
   (get (.data orswot) k default)))

(defn orswot-conj
  "Takes an orswot, a node, a local-version, and list of key value pairs"
  ([orswot node k]
   (let [version' (crdt/vclock-inc (.version orswot) node)
         data' (update-in (.data orswot)
                          [k]
                          (fnil crdt/vclock-merge {})
                          {node (get version' node)})]
     (Orswot. version' data' (.metadata orswot))))
  ([orswot node k & more]
   (reduce #(orswot-conj %1 node %2) (orswot-conj node k) more)))

(defn orswot-disj
  ([orswot node k]
   ;; TODO: make these asserts validate at a higher level
   ;; (assert (< (get-in orswot [:version node] -1) local-version))
   (let [version' (crdt/vclock-inc (.version orswot) node)
         data' (dissoc (.data orswot) k)]
     (Orswot. version' data' (.metadata orswot))))
  ([orswot node k & ks]
   (reduce #(orswot-disj %1 node %2) (orswot-disj node k) ks)))

(defn orswot-merge-disjoint-keys
  [unique-data other-side-clock result]
  (reduce-kv (fn [result unique-key unique-vclock]
               (if (crdt/vclock-descends other-side-clock unique-vclock)
                 result
                 (assoc result unique-key (crdt/vclock-prune unique-vclock other-side-clock))))
             result
             unique-data))

(defn orswot-merge-common-keys
  [left-version left-entries right-version right-entries result]
  (reduce-kv (fn [result key left-dots]
               (let [right-dots (get right-entries key)
                     common-dots (reduce-kv (fn [common node time]
                                              (if (= time (get left-dots node))
                                                (assoc common node time)
                                                common))
                                            {}
                                            right-dots)
                     left-unique (apply dissoc left-dots (keys common-dots))
                     right-unique (apply dissoc right-dots (keys common-dots))
                     left-keep (crdt/vclock-prune left-unique right-version)
                     right-keep (crdt/vclock-prune right-unique left-version)
                     new-dots (crdt/vclock-merge common-dots left-keep right-keep)]
                 (if (empty? new-dots)
                   (dissoc result key)
                   (assoc result key new-dots))))
             result
             left-entries))

(defn orswot-resolve
  [orswot1 orswot2]
  (let [keys-in-left (.data orswot1)
        keys-in-right (.data orswot2)
        only-in-left (remove (partial contains? keys-in-right) (keys keys-in-left))
        only-in-right (remove (partial contains? keys-in-left) (keys keys-in-right))
        keys-in-both (filter (partial contains? keys-in-right) (keys keys-in-left))
        merged-version (crdt/vclock-merge (.version orswot1) (.version orswot2))
        merged-data (orswot-merge-disjoint-keys
                      (select-keys keys-in-left only-in-left)
                      (.version orswot2)
                      {})
        merged-data' (orswot-merge-disjoint-keys
                       (select-keys keys-in-right only-in-right)
                       (.version orswot1)
                       merged-data)
        merged-data'' (orswot-merge-common-keys
                        (.version orswot1)
                        (select-keys keys-in-left keys-in-both)
                        (.version orswot2)
                        (select-keys keys-in-right keys-in-both)
                        merged-data')]
    (Orswot. merged-version merged-data'' (merge (.metadata orswot1)
                                                 (.metadata orswot2)))))

(defn orswot-stats
  [orswot]
  {:actor-count (count (.version orswot))
   :element-count (count (.data orswot))
   :max-dot-length (apply max 0 (map count (vals (.data orswot))))})
