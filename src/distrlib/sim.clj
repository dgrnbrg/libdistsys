(ns distrlib.sim
  (:refer-clojure :exclude [shuffle])
  (:require [clojure.data.priority-map :refer (priority-map-keyfn)]
            [yesql.core :refer (defqueries)]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [schema.core :as s]
            [clojure.java.jdbc :as jdbc])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defrecord Message [time dest id])

(defrecord Timeout [time dest id])

(defn message
  [target & kvs]
  (let [m (->Message nil target nil)]
    (if kvs
      (apply assoc m kvs)
      m)))

(defn timeout
  [t & kvs]
  (let [m (->Timeout t nil nil)]
    (if kvs
      (apply assoc m kvs)
      m)))

(defn timeout?
  [t]
  (instance? Timeout t))

(defn message?
  [m]
  (instance? Message m))

(defn event?
  [e]
  (or (message? e) (timeout? e)))

(let [a (atom 0)]
  (defn unique
    []
    (swap! a inc)))

(defn assoc-if-nil
  [m & kvs]
  (reduce (fn [m [k v]]
            (if (get m k)
              m
              (assoc m k v)))
          m
          (partition 2 kvs)))

(defn network-delay
  [seed]
  (+ (if (zero? (mod seed 2))
       1
       500) (mod seed 10)))

(defn fixup-events
  [node now events]
  (map (fn [e]
         (assoc (cond
                  (instance? Message e)
                  (assoc-if-nil e :time (inc now) #_(+ (network-delay (hash [node now e])) 50 now))
                  (instance? Timeout e)
                  (-> e
                      (assoc-if-nil :dest node)
                      (update-in [:time] #(+ now %))) ; deliver to self
                  )
                :id (unique)))
       events))

(defrecord SimulatorState [^long time ^long seed node q])
;;TODO stick this record into every actor callback arg
;;ensure that the run lets you prepopulate this state w/ whatever crap you want
;;perhaps a seed should be stored here, too, since we'd really like to have
;;deterministc randomness. We could include builtins like shuffles, RNGs, etc
;;probably need a decent hash mixing fn for longs too

(defn random-from
  "Returns a random number based on the given simulator state"
  [state]
  (hash [(:time state) (:seed state) (:node state)]))

(defn log
  "Logs a message to the DB from the given algo"
  [state & msgs]
  (when-let [q (:q state)]
    (async/>!! q [:log {:sim-time (:time state)
                        :node (:node state)
                        :msg (str/join \space msgs)}])))

(def positive-int (s/both long (s/pred (comp not neg?) 'positive)))

(defn non-empty-coll
  [schema]
  (s/both schema
          (s/pred seq 'non-empty)))

;;; The following are validators for schema for the simulator's extra state
(def extra-state-schema
  {(s/both s/Keyword
           (s/pred #(not (namespace %)) 'without-namespace)
           (s/pred #(re-matches #"^[a-zA-Z0-9_]+$" (name %)) 'safe-for-db))
   (s/enum :str :kw :int :obj)})

(def sim-config-map
  {(s/required-key :actors) (non-empty-coll {s/Keyword (s/pred fn? 'function)})
   (s/required-key :initial-events) (non-empty-coll [(s/pred event? 'event)])
   (s/required-key :until-time) positive-int
   (s/optional-key :seed) long
   (s/optional-key :validating?) boolean
   (s/optional-key :store) {(s/required-key :file) s/Str
                            (s/optional-key :overwrite) boolean
                            ;; TODO make logging events/state optional
                            }
   (s/optional-key :event-schema) extra-state-schema
   (s/optional-key :state-schema) extra-state-schema})

(defmacro event-validor-helper
  "Used for simplifying make-event-validator"
  [pred k what]
  `(fn [event#]
     (if (contains? event# ~k)
       (or (~pred (get event# ~k))
           (str ~k " should be a " ~what ", was " (get event# ~k)))
       true)))

(defn make-event-validator
  "Throws if it sees an event that isn't properly specified (i.e. must
   have all keys and no extras). Takes the built-in properties + the event
   schema into consideration."
  [event-schema]
  {:pre [(map? event-schema)
         (not (contains? event-schema :id))
         (not (contains? event-schema :time))
         (not (contains? event-schema :dest))]}
  (let [event-schema (assoc event-schema
                            :time :int
                            :dest :kw)
        validators (map (fn [[k t]]
                          (case t
                            :str (event-validor-helper string? k "string")
                            :kw (event-validor-helper keyword? k "keyword")
                            :int (event-validor-helper integer? k "int")
                            :obj (constantly true)))
                        event-schema)
        keyset (conj (set (keys event-schema)) :id)]
    (fn validate [e]
      (when-not (every? keyset (keys e))
        (throw (ex-info "Event had extra keys" {:extra-keys (remove keyset (keys e))})))
      (when-let [errors (seq (reduce (fn [errors v]
                                       (let [x (v e)]
                                         (if (string? x)
                                         (conj errors x)
                                         errors)))
                                     []
                                     validators))]
        (throw (ex-info (str "Keys failed to validate " errors) {:errors errors})))
      e)))

;;; TODO: make a visual environment that shows the execution traces rendered as graphs/timeseries
;;; Need a way to generate logs and view them efficiently on the trace. We'll do this by buffering each step in memory, and then flushing out to a log file (leveldb or sqlite). We'll be able to then serve up the log through another module, which initially will give you a table control that lets you scroll through all events & hover to get datastructure details
;;; Use https://highlightjs.org/usage/ for datastructure higlighting?
;;; Should capture every time an event runs, in order. Info includes the node, the message, the time, the start & end state, every log inside the actor, and all generated messages. Should use clojure.tools.logging but need to include richer tracking metadata to ensure the log is useful for the analyzer (in text or db?)

(defn sim-schema->ddl
  [schema]
  (map (fn [[column type]]
         [column (get {:int :int
                       :str "varchar(32)"
                       :obj :blob
                       :kw "varchar(32)"} type)])
       schema))

(defn state-table
  "Stores the series of all states in the simulator. The agents
   can provide additional state to be stored in each state snapshot"
  [schema]
  (apply
    jdbc/create-table-ddl
    :primary_state
    [:time :int] ;; represents the simulation time (sparse)
    [:step :int] ;; represents the step (dense)
    [:node :int] ;; whose state is this?
    (sim-schema->ddl schema)))

(defn event-table
  "Stores the series of all events in the simulator."
  [schema]
  (apply
    jdbc/create-table-ddl
    :primary_events
    ;;TODO store delay upfront?
    [:sender_state :int] ;; id of state of sending node
    [:destination_node :int] ;; who should recieve this?
    [:reciever_state :int] ;; id of state of receiving node, set upon delivery
    (sim-schema->ddl schema)))

(defn serialize-map
  [seed schema state]
  {:pre [(map? schema) (or (nil? state) (map? state))]}
  (reduce-kv (fn [result k v]
               (assoc result k (case (get schema k)
                                 :str v
                                 :obj (pr-str v)
                                 :int v
                                 :kw (name v))))
             seed
             state))

(defn deserialize-map
  [schema state]
  {:pre [(map? schema)]}
  (reduce-kv (fn [result k v]
               (assoc result k (case (get schema k)
                                 :obj (read-string v)
                                 :kw (keyword v)
                                 v)))
             {}
             state))

(defqueries "sql/queries.sql")

;;; TODO put a cache on some wrapped db fns, like find-node-in-db

(defn lookup-node-name->id
  "Converts a node name to an id"
  [db node-name]
  (-> (lookup-node-name->id* db (name node-name))
      first
      :rowid))

(defn lookup-node-id->name
  "Converts a node id to a name"
  [db node-id]
  (-> (lookup-node-id->name* db node-id)
      first
      :name))

(defn store-state-for-node!
  [db schema sim-time sim-step node state]
  (let [id (lookup-node-name->id db node)]
    (-> (jdbc/insert! db :primary_state (serialize-map
                                      {:time sim-time
                                       :step sim-step
                                       :node id}
                                      schema
                                      state))
        first
        (get (keyword "last_insert_rowid()")))))

(defn insert-event!
  [db event-schema event sender-state]
  (let [dest-id (lookup-node-name->id db (:dest event))]
    (-> (jdbc/insert! db :primary_events (serialize-map
                                           {:sender_state sender-state
                                            :destination_node dest-id}
                                           event-schema
                                           (dissoc (into {} event) :time :dest :id)))
        first
        (get (keyword "last_insert_rowid()")))))

(def log-table
  (jdbc/create-table-ddl
    :user_logs
    [:node :int]
    [:time :int]
    [:msg "varchar(255)"]))

(defn store-log-for-node!
  "Stores a log into the log table"
  [db time node msg]
  (-> (jdbc/insert! db :user_logs {:time time
                                   :node (lookup-node-name->id db node)
                                   :msg msg})
      first
      (get (keyword "last_insert_rowid()"))))

(def node-table
  (jdbc/create-table-ddl
    :node_names
    [:name "varchar(32)"]))

(defn populate-node-table!
  "Nodes should be a seq of keywords"
  [db nodes]
  (apply jdbc/insert! db :node_names (map (fn [n] {:name (name n)}) nodes)))

(defn remove-nil-values
  [m]
  (reduce-kv (fn [m k v]
               (if-not (nil? v)
                 (assoc m k v)
                 m))
             {}
             m))

(defn get-all-states
  "Returns all the state data, joined with node names"
  [db schema]
  (map #(-> (deserialize-map schema %)
            (dissoc :node)
            (remove-nil-values))
       (get-all-states* db)))

(defn latest-state-id-for-node
  "Finds the rowid of the most recent state for a node as of a given time"
  [db node-name time]
  (-> (latest-state-id-for-node* db (name node-name) time)
      (first)
      (:rowid)))

(defn get-all-events
  [db event-schema state-schema]
  (->> (get-all-events* db)
       (map (fn [raw-event]
              (let [sender-state-raw (first (get-state-by-state-id
                                              db
                                              (:sender_state raw-event)))
                    sender-state (-> sender-state-raw
                                     (update-in [:node]
                                                (partial lookup-node-id->name db))
                                     (remove-nil-values))
                    recvr-state-raw (first (get-state-by-state-id
                                             db
                                             (:reciever_state raw-event)))
                    recvr-state (-> recvr-state-raw
                                    (update-in [:node]
                                               (partial lookup-node-id->name db))
                                    (remove-nil-values))
                    sender-logs (get-logs db
                                          (:time sender-state-raw)
                                          (:node sender-state-raw))
                    recvr-logs (get-logs db
                                         (:time recvr-state-raw)
                                         (:node recvr-state-raw))]
                (-> raw-event
                    (dissoc :sender_state :destination_node :reciever_state)
                    (assoc :sender sender-state
                           :reciever recvr-state
                           :dest (lookup-node-id->name
                                   db (:destination_node raw-event))
                           :reciever-logs recvr-logs
                           :sender-logs sender-logs)
                    (remove-nil-values)))))))

(defn sqlite-db-spec
  "Returns a db spec for a sqlite db"
  [path]
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname path})

(defn make-db-pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))]
    {:datasource cpds}))

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

(defn check-start-or-close-event
  "Throws if the given event isn't a start event"
  [[tag node] db]
  (if (= tag :close)
    (do (.close db)
        false)
    (do (when (not= tag :start-event)
          (throw (ex-info "Not a start event" {:tag tag :node node})))
        node)))

(defn batch-requests
  "Reads from a channel until it sees a :flush value, then returns the
   accumuluated buffer"
  [chan]
  (async/go
    (loop [buf []]
      (let [e (async/<! chan)]
        (if (= e :flush)
          buf
          (recur (conj buf e)))))))

(defn reduce-db-event
  "Curried reducing function. First, you pass it the config & database,
   then, it returns the reducing function which will work on a sequence
   of tagged events, adding them to the db and updating the tracking state."
  [config db]
  (fn reduce-db-event-impl [new-state [action args]]
    (case action
      :log
      (do
        (store-log-for-node!
          db
          (:sim-time args)
          (:node args)
          (:msg args))
        new-state)
      :store-state-for-node
      (assoc-in new-state
                [:node-states (:node args)]
                (store-state-for-node!
                  db
                  (:state-schema config)
                  (:sim-time args)
                  (:sim-step args)
                  (:node args)
                  (:state args)))
      :insert-event
      (assoc-in new-state
                [:undelivered-events
                 (:id (:event args))]
                (insert-event!
                  db
                  (:event-schema config)
                  (:event args)
                  (get-in new-state
                          [:node-states (:sender args)])))
      :add-reciever-to-event
      (do
        (add-reciever-to-event!
          db
          (get-in new-state [:node-states
                             (:reciever-node args)])
          (get-in new-state [:undelivered-events
                             (:event-id args)]))
        (dissoc-in new-state
                   [:undelivered-events
                    (:event-id args)])))))

(defn init-db
  "Intializes the sqlite db and returns the connection or nil if not configured"
  [config]
  (when (:store config)
    (let [path (get-in config [:store :file])
          db (make-db-pool (sqlite-db-spec path))
          q (async/chan (async/buffer 10000))]
      (when (and (not (get-in config [:store :overwrite]))
                 (.exists (java.io.File. path)))
        (throw (ex-info (str "File already exists:" path) {})))
      (.delete (java.io.File. path))
      (jdbc/db-do-commands
        db
        node-table
        log-table
        (state-table (:state-schema config))
        (event-table (:event-schema config)))
      (populate-node-table! db (keys (:actors config)))
      (async/go
        (loop [state {:node-states {}
                      :undelivered-events {}}]
          (when-let [node (check-start-or-close-event (async/<! q) (:datasource db))]
            (let [reqs (async/<! (batch-requests q))]
              (recur (reduce (reduce-db-event config db) state reqs))))))
      q)))

(defn run
  "Takes a map from node names to actors"
  ([{:keys [actors initial-events until-time event-schema state-schema] :as config}]
   (s/validate sim-config-map config)
   (let [seed (:seed config 22)
         validating? (:validating? config true)
         db-chan (init-db config)
         event-validator (make-event-validator event-schema)
         state-validator (make-event-validator state-schema)
         initial-node-states (into {} (map (fn [[node actor]] [node (actor)]) actors))
         initial-events' (->> initial-events
                              (fixup-events ::root -1)
                              (mapcat (fn [e] [(:id e) e])))]
     (loop [current-time 0
            prev-time 0
            iters 0
            current-node-states initial-node-states
            pending-events (apply priority-map-keyfn :time initial-events')]
       (if (and (not (neg? current-time))
                (< current-time until-time))
         (let [events-at-this-time (take-while #(= (:time %) current-time) (vals pending-events))
               [next-state new-events]
               (reduce (fn [[state events] e]
                         (let [node (:dest e)
                               _ (when db-chan (async/>!! db-chan [:start-event node]))
                               actor (get actors node)
                               node-state (get state node)
                               [node-state' new-events] (actor node-state (->SimulatorState current-time seed node db-chan) e)
                               fixed-events (fixup-events node current-time new-events)]
                           (when validating?
                             (doseq [e fixed-events]
                               (event-validator e))
                             (state-validator node-state'))
                           (when db-chan
                             (async/>!! db-chan
                                   [:store-state-for-node
                                    {:sim-time current-time
                                     :sim-step iters
                                     :node node
                                     :state node-state'}])
                             (async/>!! db-chan
                                   [:add-reciever-to-event
                                    {:event-id (:id e)
                                     :reciever-node node}])
                             (doseq [e fixed-events]
                               (async/>!! db-chan [:insert-event
                                         {:event e
                                          :sender node}]))
                             (async/>!! db-chan :flush))
                           [(assoc state node node-state')
                            (into events fixed-events)]))
                       [current-node-states []]
                       events-at-this-time)
               next-events (into (apply dissoc pending-events (map :id events-at-this-time))
                                 (map (fn [e] [(:id e) e]) new-events))
               next-time (:time (first (vals next-events)) -1)]
           (recur next-time current-time (inc iters) next-state next-events))
         (do (when db-chan (async/>!! db-chan [:close]))
             [prev-time current-node-states]))))))
