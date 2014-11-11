(ns distrlib.crdt
  (:refer-clojure :exclude (resolve type)))

(defprotocol CRDT
  (type [this] "Returns a keyword encoding the type")
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

;;; vector clock is a map from node to time
;;; must be able to add a dot to a version
;;; must be able to get the time of a node in a version
;;; must be able to merge versions
;;; must be able to drop dots that are dominated by a vclock

(defn vclock-merge
  [& vclocks]
  (apply merge-with max vclocks))

(defn vclock-inc
  [vclock node]
  (update-in vclock [node] (fnil inc 0)))

(defn vclock-prune
  "Returns the left-side vclock without all entries that
   are dominated by the right-side vclock (used for keeping dotted
   elements small and causal)"
  [lvclock rvclock]
  (reduce-kv (fn [new-vclock left-node time]
               (if (>= (get rvclock left-node -1) time)
                 new-vclock
                 (assoc new-vclock left-node time)))
             {}
             lvclock))

(defn vclock-descends
  "Returns true if va is a direct descendent of vb. A vclock is its own descendent."
  [va vb]
  (if (empty? vb)
    true
    (every? #(<= (val %) (get va (key %) -1)) vb)))

(def ^:dynamic *current-node* (java.util.UUID/randomUUID))

(defmacro with-node
  [node & body]
  `(binding [*current-node* ~node] ~@body))
