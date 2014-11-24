(ns distrlib.sim-test
  (:require [distrlib.sim :as sim]
            [clojure.test.check.clojure-test :refer (defspec)]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]))

(defn broadcaster
  "This returns a sim actor that sends the payload to all the targets at each
   interval"
  [targets payload interval]
  (fn
    ([] nil)
    ([state _ msg]
     [state (conj (map (fn [node] (merge (sim/message node)
                                         payload))
                       targets)
                  (sim/timeout interval))])))

(defn counter
  "This is a sim actor that counts how many messages it recieved"
  ([] 0)
  ([count _ msg]
   [(inc count)]))

(defn t []
  (sim/run {:broadcaster (broadcaster [:n1 :n2 :n3] {:lol :hi} 2)
            :n1 counter
            :n2 counter
            :n3 counter
            :n4 counter}
           [(sim/message :broadcaster)]
           5))

(defn run-broadcast-sim
  [limit frequency]
  (sim/run {:broadcaster (broadcaster [:n1 :n2 :n3] {:lol :hi} frequency)
                                       :n1 counter
                                       :n2 counter
                                       :n3 counter
                                       :n4 counter}
                                      [(sim/message :broadcaster)]
                                      limit))

(def never-sim-past-time-limit
  (prop/for-all [[limit interval] (gen/bind
                                    (gen/choose 10 500)
                                    #(gen/tuple (gen/return %)
                                                (gen/choose 1 (quot % 10))))]
                (assert (<= (* 10 interval) limit) "too few messages will be sent!")
                (let [[cycle result] (run-broadcast-sim limit interval)]
                  (and (<= cycle limit) ; Ensure that the sim doesn't run too far
                       (= (:n1 result) (:n2 result) (:n3 result)) ; Ensure sim does stuff
                       (pos? (:n1 result))))))

(defspec check-never-sim-past-time-limit
  100
  never-sim-past-time-limit)

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

(defn gossiper
  "Takes a collection of other nodes in the cluster"
  [peers]
  (fn
    ([] #{}) ; init empty state
    ([state simulator-state msg]
     (let [msg-stamp (:stamp msg)
           state' (if (contains? state msg-stamp)
                    state
                    (conj state msg-stamp))
           fwd-peers (take (inc (long (Math/ceil (count peers))))
                           (nth-permutation peers (hash simulator-state)))
           fwd-msgs (map (fn [p] (sim/message p :stamp msg-stamp)) fwd-peers)]
       [state' (when (not= state state') fwd-msgs)]))))

(defn construct-gossipers
  [n]
  (let [names (set (map #(keyword (str "node" %)) (range n)))]
    (into {}
          (map (fn [n]
                 [n (gossiper (disj names n))]) names))))

(def simple-gossip-network
  (prop/for-all [n-participants (gen/choose 3 300)
                 seed gen/int]
                (let [[cycles results] (sim/run seed
                                                (construct-gossipers 5)
                                                [(sim/message :node1 :stamp "hello")]
                                                5)]
                  (and (apply = (vals results)) ; ensure all recv msg
                       (= #{"hello"} (first (vals results))) ; ensure recv a msg
                       ))))

(defspec check-simple-gossip-network
  1000
  simple-gossip-network)
