(ns distrlib.orswot
  (:refer-clojure :exclude (resolve)))

;; Ideas:
;; - Update operations should only take the node, not the time, because the time
;; is implied by the version vector of the update (maybe not true; try to prove)
;; - Version vector can live outside any specific CRDT for efficiency/sharing in
;; combined structures (could be hard b/c orswot leverages this heavily?)
;; - share code between DVV, orswot, and ormwot

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

;; A dot is a node & a local time for that node.
;; version vectors are collections of dots. some
;; optimized CRDTs, like orswot & dvv use them as well
(defrecord Dot [node time])

(defrecord Version [])

(defn dot?
  [x]
  (instance? Dot x))

(defn version?
  [x]
  (instance? Version x))

;; TODO these 3 should work for all combinations of versions and dots
;; case 1: 2 versions
;; case 2: 2 dots
;; case 3: version + dot should try to cast the version to a dot, then compare there

(defn cast-version-to-dot
  "Tries to extract the relevant dot from the version that matches the given
   node."
  [node version]
  (when-let [time (get version node)]
    (->Dot node time)))

(defn before?
  "Returns true if this happened before or equal to that"
  [this that]
  (cond
    (and (dot? this) (dot? that)) (and (= (:node this) (:node that))
                                       (<= (:time this) (:time that)))
    (and (version? this) (version? that)) (reduce (fn [b k]
                                                    (and b (<= (get this k)
                                                              (get that k))))
                                                  true
                                                  (keys this))
    (and (dot? this) (version? that)) (before? this (cast-version-to-dot
                                                      (:node this) that))
    (and (version? this) (dot? that)) (before? (cast-version-to-dot
                                                 (:node that) this)
                                               that)))

(defn after?
  "Returns true if this happened after that"
  [this that])

(defn concurrent?
  "Returns true if this happened concurrently with that"
  [this that])

(defn update-version
  "Takes a version and a dot, and increases the version"
  []
  )

(def ^:dynamic *local-counter*)

(def ^:dynamic *dot* (->Dot :n1 0))

(def ^:dynamic *dot-source*)

(defn make-dot-source
  "Makes a function that yields sequential dots when called"
  ([node]
   (make-dot-source node 0))
  ([node start]
   (let [a (atom start)]
     (fn next-dot [] (->Dot node (swap! a inc))))))

(defmacro with-dot
  [dot & body]
  `(binding [*dot* ~dot] ~@body))

(defmacro with-dot-source
  [dot-source & body]
  `(binding [*dot-source* ~dot-source] ~@body))
  

(defn current-dot
  []
  (cond (bound? #'*dot*) *dot*
        (bound? #'*dot-source*) (*dot-source*)
        :else (throw (ex-info "No dot in dynamic binding!" {}))))

(declare orswot-get orswot-conj orswot-disj orswot-resolve)

;; version is a map from node ID to local version
;; data is a map from keys to Value pairs
(deftype Orswot [version data metadata]
  clojure.lang.IPersistentSet
  (disjoin [this o]
    (orswot-disj this (current-dot) o))
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
    (orswot-conj this (current-dot) o))
  (empty [this]
    (Orswot. version {} metadata))

  clojure.lang.Counted
  (count [this]
    (count (.data this)))

  clojure.lang.Seqable
  (seq [this]
    (keys data))
  
  CRDT
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
  ([orswot dot k]
;;     (assert (< (get-in orswot [:version node] -1) local-version))
   (let [version' (assoc (.version orswot) (:node dot) (:time dot))
         data' (assoc (.data orswot) k dot)]
     (Orswot. version' data' (.metadata orswot))))
  ([orswot dot k & more]
   (reduce #(orswot-conj %1 dot %2) (orswot-conj dot k) more)))

(defn orswot-disj
  ([orswot dot k]
   ;; TODO: make these asserts validate at a higher level
   ;; (assert (< (get-in orswot [:version node] -1) local-version))
   (let [version' (assoc (.version orswot) (:node dot) (:time dot))
         data' (dissoc (.data orswot) k)]
     (Orswot. version' data' (.metadata orswot))))
  ([orswot dot k & ks]
   (reduce #(orswot-disj %1 dot %2) (orswot-disj dot k) ks)))

(defn orswot-resolve
  [orswot1 orswot2]
  (let [keys-in-left (.data orswot1)
        keys-in-right (.data orswot2)
        only-in-left (remove (partial contains? keys-in-right) (keys keys-in-left))
        only-in-right (remove (partial contains? keys-in-left) (keys keys-in-right))
        kept-left (filter (fn [k]
                            (let [{:keys [node time]} (get keys-in-left k)]
                              (> time (get (.version orswot2) node -1))))
                          only-in-left)
        kept-right (filter (fn [k]
                             (let [{:keys [node time]} (get keys-in-right k)]
                               (> time (get (.version orswot1) node -1))))
                           only-in-right)
        keys-in-both (filter (partial contains? keys-in-right) (keys keys-in-left))
        merged-version (merge-with max (.version orswot1) (.version orswot2))
        ;;TODO handle value conflicts
        merged-both (merge (select-keys keys-in-left (concat kept-left keys-in-both))
                           (select-keys keys-in-right kept-right))]
    (Orswot. merged-version merged-both (merge (.metadata orswot1)
                                               (.metadata orswot2)))))
