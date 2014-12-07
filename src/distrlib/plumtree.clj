(ns distrlib.plumtree
  (:require [distrlib.sim :as sim]))

;; Taken from http://stackoverflow.com/questions/14836414/can-i-make-a-deterministic-shuffle-in-clojure

(def factorial (reductions * 1 (drop 1 (range))))

(defn factoradic [n] {:pre [(>= n 0)]}
   (loop [a (list 0) n n p 2]
      (if (zero? n) a (recur (conj a (mod n p)) (quot n p) (inc p)))))

(defn nth-permutation [s n]
  (let [n (mod n (nth factorial (count s)))
        d (factoradic n)
        choices (concat (repeat (- (count s) (count d)) 0) d)]
    ((reduce
        (fn [m i]
          (let [[left [item & right]] (split-at i (m :rem))]
            (assoc m :rem (concat left right)
                     :acc (conj (m :acc) item))))
      {:rem s :acc []} choices) :acc)))

;; Taken from http://stackoverflow.com/a/14488425
(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn plumtree-node
  "A plumtree node has the following state:
   
   - `peers`: the set of other nodes in the cluster
   - `push-peers`: the list of other nodes that we push to
   - `messages`: map from msg ids to msgs
   - `pending-by`: map from msg ids to the first peer that we heard from
   - `upstream`: the peer that we believe we're linked to
   
   i-have messages are the lazy type, gossip messages are the eager type

   i-have messages look like: `{:i-have id :source peer}`

   gossip messages look like: `{:gossip id :value msg-body :source peer}`

   prune messages look like: `{:prune peer}`

   graft messages look like: `{:graft id :source peer}`

   The plumtree algorithm is as follows:

   - [todo] If we recieve a message about a new node, we add it to the peers set
   - If we recieve a i-have message, we add it to the pending-by and set a timer
   - If we recieve an gossip message, we add it to the messages and remove from pending-by
   - If we recieve a timeout, we send a prune to `upstream` and `graft` to the `pending-by` target
   - If we recieve a `prune`, we remove the peer from the `push-peers`
   - If we recieve a `graft`, we add the peer to the `push-peers` and include the target message"
  [self peers timeout]
  (fn
    ([]
     {:push-peers #{}
      :messages {}
      :pending-by {}
      :upstream nil})
    ([{:keys [push-peers messages pending-by upstream] :as state} simulator-state msg]
     (cond
       (:i-have msg)
       (let [id (:i-have msg)]
         (if (and (not (contains? messages id))
                  (not (contains? pending-by id)))
           [(assoc-in state [:pending-by id] (:source msg))
            [(sim/timeout timeout :msg id)]]
           [state []]))

       (:gossip msg)
       (let [id (:gossip msg)
             body (:value msg)
             source (:source msg)]
         ;(println "gossiped" msg)
         (if-not (contains? messages id)
           ;;TODO should we graft/prune if needed, here?
           [(-> state
                (assoc-in [:messages id] body)
                (dissoc-in [:pending-by id]))
            (concat
              (for [peer push-peers]
                (sim/message peer :gossip id :value body :source self))
              (for [peer peers #_(take (max (- (+ 2 (long (Math/ceil (Math/log (count peers)))))
                                              (count push-peers))
                                    0)
                               (nth-permutation peers (sim/random-from simulator-state)))]
                (sim/message peer :i-have id :source self)))]
           [state #_(if-not (= ::client source)
                    [(sim/message source :prune self)]
                    [])]))

       (sim/timeout? msg)
       (let [id (:msg msg)]
         (println "Triggered timeout" msg)
         [state
          (concat
            (when-let [new-upstream (get pending-by id)]
              [(sim/message new-upstream
                            :graft id
                            :source self)])
            (when upstream
              (println "pruning due to existing upstream")
              [(sim/message upstream
                            :prune self)]))])

       (:graft msg)
       (let [id (:graft msg)
             peer (:source msg)]
         (println "Grafting:" msg)
         [(update-in state [:push-peers] conj peer)
          [(sim/message peer
                        :gossip id
                        :source self
                        :value (get messages id))]])
       
       (:prune msg)
       (do
         (println "Pruning" msg)
         [(update-in state [:push-peers] disj (:prune msg))])

       :else
       (do
         (println "got bad message" msg)
         (flush)
         [state])
       ))))

(defn construct-gossipers
  [n]
  (let [names (set (map #(keyword (str "node" %)) (range n)))]
    (into {}
          (map (fn [n]
                 [n (plumtree-node n (disj names n) 10)]) names))))

(defn calc-push-tree-depth
  [root result]
  (let [total (count result)]
    (apply max (map first (take total
          (iterate
            (fn [[depth nodes]]
              (let [new-nodes (into nodes (mapcat #(get-in result [% :push-peers]) nodes))]
                [(if (not= nodes new-nodes) (inc depth) depth)
                 new-nodes]))
            [0 #{root}]))))))

(comment
  (dotimes [i 100])
    (let [seed (rand-int 1000)
          _ (println "Seed was" seed)
          [c result]
        (sim/run seed (construct-gossipers 20)
                 [(sim/message :node1 :gossip 1 :value "message 1" :source ::client)
                  ;(sim/message :node2 :time 1 :gossip 2 :value "message 2" :source ::client) 
                  ;(sim/message :node3 :time 2 :gossip 3 :value "message 3" :source ::client) 
                  ;(sim/message :node4 :time 3 :gossip 4 :value "message 4" :source ::client) 
                  ]
                 5000)]
    (println "took" c "cycles")
    (println "tree depth was" (calc-push-tree-depth :node1 result))
    (clojure.pprint/pprint result)
    )
  )
