(ns distrlib.orswot
  (:refer-clojure :exclude (assoc dissoc merge get))
  (:require [potemkin :refer (def-map-type)]))

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
  [orswot k]
  (get-in orswot [:data k]))

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
         _ (println "hi")
         merged-version (merge-with max (:version orswot1 -1) (:version orswot2 -1))
         _ (println "hi")
         ;;TODO handle value conflicts
         merged-both (clojure.core/merge (select-keys keys-in-left (concat kept-left keys-in-both))
                                         (select-keys keys-in-right kept-right))]
          (println "hi")
     (->Orswot merged-version merged-both)))
  ([o1 o2 & os]
   (reduce merge (list* o1 o2 os))))

(def x1 (-> (orswot)
            (assoc :n1 0 "hello")
            (assoc :n1 0 "hello")
            ))

(def x2 (-> (orswot)
            (assoc :n2 1 "hi")
            ))

(def x3
  (-> (orswot)
      (assoc :n2 1 "hi")
      (dissoc :n2 2 "hi")
      ))

(merge x1 x2 x3)
