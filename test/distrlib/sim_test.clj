(ns distrlib.sim-test
  (:require [distrlib.sim :as sim]
            [clojure.test.check.clojure-test :refer (defspec)]
            [schema.core :as s]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]))

(defn broadcaster
  "This returns a sim actor that sends the payload to all the targets at each
   interval"
  [targets payload interval]
  (fn
    ([] nil)
    ([state sim msg]
     (sim/log sim "Broadcasting now!")
     [state (conj (map (fn [node] (merge (sim/message node)
                                         payload))
                       targets)
                  (sim/timeout interval))])))

(defn counter
  "This is a sim actor that counts how many messages it recieved"
  ([] {:count 0})
  ([{:keys [count]} sim msg]
   (sim/log sim "Hello at " msg)
   [{:count (inc count)}]))

(defn run-broadcast-sim
  [limit frequency]
  (sim/run {:actors {:broadcaster (broadcaster [:n1 :n2 :n3] {:lol :hi} frequency)
                     :n1 counter
                     :n2 counter
                     :n3 counter
                     :n4 counter}
            :initial-events [(sim/message :broadcaster)]
            :event-schema {:lol :kw}
            :state-schema {:count :int}
            :until-time limit}))

(deftest ^:disabled test-sql-on-broadcast
  (let [db (sim/sqlite-db-spec "broadcast.db")
        state-schema {:count :int}
        event-schema {:lol :kw}]
    (sim/run {:actors {:broadcaster (broadcaster [:n1 :n2 :n3] {:lol :hi} 5)
                       :n1 counter
                       :n2 counter
                       :n3 counter
                       :n4 counter}
              :initial-events [(sim/message :broadcaster)]
              :store {:file "broadcast.db"
                      :overwrite true}
              :event-schema event-schema
              :state-schema state-schema
              :until-time 50})
    (Thread/sleep 1000)
    (is (= 40 (count (sim/get-all-states
                       (sim/sqlite-db-spec "broadcast.db")
                       state-schema))))
    (is (= {:count 2
            :node 2
            :step 3
            :time 6}
           (first (sim/get-state-by-state-id
                    db
                    (sim/latest-state-id-for-node
                      db
                      :n1
                      10)))))
    (is (= {:count 3
            :node 2
            :step 5
            :time 11}
           (first (sim/get-state-by-state-id
                    db
                    (sim/latest-state-id-for-node
                      db :n1 11)))))))

(comment
  ;; This is for playing with simple db features
  (time (sim/run {:actors {:broadcaster (broadcaster [:n1 :n2 :n3] {:lol :hi} 5)
                     :n1 counter
                     :n2 counter
                     :n3 counter
                     :n4 counter}
            :initial-events [(sim/message :broadcaster)]
            :store {:file "broadcast.db" :overwrite true}
            :event-schema {:lol :kw}
            :state-schema {:count :int}
            :until-time 50}))

  (clojure.pprint/pprint
    (jdbc/query (sim/sqlite-db-spec "broadcast.db")
                "SELECT * FROM user_logs"))

  (clojure.pprint/pprint (sim/get-all-events
    (sim/sqlite-db-spec "broadcast.db")
    {:lol :kw}
    {:count :int}))
  
  (clojure.pprint/pprint
    (sim/get-all-states
      (sim/sqlite-db-spec "broadcast2.db")
      {:count :int}))

  (clojure.pprint/pprint
    (sim/get-state-by-state-id
      (sim/sqlite-db-spec "broadcast.db")
      (sim/latest-state-id-for-node
                       (sim/sqlite-db-spec "broadcast.db")
                       "n1"
                       10
                       ))
    )

  (jdbc/query (sim/sqlite-db-spec "broadcast.db") "SELECT * FROM primary_events")
  (sim/get-all-events* (sim/sqlite-db-spec "broadcast.db"))

  (clojure.pprint/pprint
    (sim/get-all-events* 
      (sim/sqlite-db-spec "broadcast.db"))
    )

  

  )

(def never-sim-past-time-limit
  (prop/for-all [[limit interval] (gen/bind
                                    (gen/choose 10 500)
                                    #(gen/tuple (gen/return %)
                                                (gen/choose 1 (quot % 10))))]
                (assert (<= (* 10 interval) limit) "too few messages will be sent!")
                (let [[cycle result] (run-broadcast-sim limit interval)]
                  (and (<= cycle limit) ; Ensure that the sim doesn't run too far
                       (= (get-in result [:n1 :count])
                          (get-in result [:n2 :count])
                          (get-in result [:n3 :count])) ; Ensure sim does stuff
                       (pos? (get-in result [:n1 :count]))))))

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
    ([] {:state #{}}) ; init empty state
    ([{:keys [state]} simulator-state msg]
     (let [msg-stamp (:stamp msg)
           state' (if (contains? state msg-stamp)
                    state
                    (conj state msg-stamp))
           fwd-peers (take (+ 2 (long (Math/ceil (Math/log (count peers)))))
                           (nth-permutation peers (sim/random-from simulator-state)))
           fwd-msgs (map (fn [p] (sim/message p :stamp msg-stamp)) fwd-peers)]
       [{:state state'} (when (not= state state') fwd-msgs)]))))

(defn construct-gossipers
  [n]
  (let [names (set (map #(keyword (str "node" %)) (range n)))]
    (into {}
          (map (fn [n]
                 [n (gossiper (disj names n))]) names))))

(def simple-gossip-network
  (prop/for-all [n-participants (gen/choose 3 300)
                 seed gen/int]
                (let [[cycles results] (sim/run {:seed seed
                                                 :actors (construct-gossipers 5)
                                                 :initial-events [(sim/message :node1 :stamp "hello")]
                                                 :event-schema {:stamp :str}
                                                 :state-schema {:state :obj}
                                                 :until-time 5})]
                  (and (apply = (map :state (vals results))) ; ensure all recv msg
                       (= #{"hello"} (:state (first (vals results)))) ; ensure recv a msg
                       ))))

(defspec check-simple-gossip-network
  10000
  simple-gossip-network)

(defmacro ex-matches
  [excerpt & code]
  `(try
     ~@code
     false
     (catch Throwable t#
       (.contains (.getMessage t#) ~excerpt))))

(deftest sim-config-map-test
  (testing "Overall sim config map"
    (is (s/validate
          sim/sim-config-map
          {:actors (construct-gossipers 3)
           :initial-events [(sim/timeout 3)]
           :until-time 10}) "working case")
    (is (s/validate
          sim/sim-config-map
          {:actors (construct-gossipers 3)
           :initial-events [(sim/timeout 3)]
           :until-time 10
           :seed 9000}) "optional seed")
    (is (ex-matches
          "{:initial-events (not (non-empty []))}"
          (s/validate
            sim/sim-config-map
            {:actors (construct-gossipers 3)
             :initial-events []
             :until-time 10})) "must have some events")
    (is (ex-matches
          "{:actors (not (non-empty {}))}"
          (s/validate
            sim/sim-config-map
            {:actors {}
             :initial-events [(sim/timeout 3)]
             :until-time 10})) "must have some actors")
    (is (ex-matches
          "{:until-time (not (positive -1))}"
          (s/validate
            sim/sim-config-map
            {:actors (construct-gossipers 3)
             :initial-events [(sim/timeout 3)]
             :until-time -1})) "must run simulation for a positive amount of time")
    (is (s/validate
          sim/extra-state-schema
          {:MyCool_Column :kw}) "simple columns match"))
  (testing "State schema specs"
    (is (thrown? Exception (s/validate sim/extra-state-schema {:my/cool_column :str}))
        "don't allow namespaces")
    (is (thrown? Exception (s/validate sim/extra-state-schema {:cool_column+ :str}))
        "don't allow special chars")))

(deftest test-event-validator
  (testing "Preconditions work"
    (is (thrown? AssertionError (sim/make-event-validator [[:foo :str]])))
    (is (thrown? AssertionError (sim/make-event-validator {:time :str})))
    (is (thrown? AssertionError (sim/make-event-validator {:id :str})))
    (is (thrown? AssertionError (sim/make-event-validator {:dest :str}))))
  (testing "Make sure the validator works"
    (let [v (sim/make-event-validator {:foo :str
                                       :bar :kw
                                       :baz :int
                                       :quux :obj})]
      (is (v {:time 22
              :dest :node1
              :foo "hello"
              :bar :whatup
              :baz 0
              :quux nil}))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #":bar should be a keyword"
                            (v {:time 22
                                :dest :node1
                                :foo "hello"
                                :bar "bar"
                                :baz 0
                                :quux [1 2 3]})))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                   #":baz should be a int"
                   (v {:time 22
                       :dest :node1
                       :foo "hello"
                       :bar :bar
                       :baz "bar"
                       :quux [1 2 3]}))))))

(deftest test-sql-basics
  (let [dbfile "test.db"
        db {:classname "org.sqlite.JDBC"
            :subprotocol "sqlite"
            :subname dbfile}
        nodes [:node1 :node2 :node3]
        schema-list [[:state :int]
                     [:myval :str]
                     [:misc :obj]]
        schema-map (into {} schema-list)]
    (.delete (java.io.File. dbfile))
    (jdbc/db-do-commands
      db
      sim/node-table
      (sim/state-table schema-map))
    (sim/populate-node-table! db nodes)
    (sim/store-state-for-node! db schema-map 20 3
                               :node2 {:misc [1 :a]})
    (let [[result] (sim/get-all-states db schema-map)]
      (is (= (:name result) "node2"))
      (is (= (:time result) 20))
      (is (= (:misc result) [1 :a]))
      (is (= (:step result) 3)))))
