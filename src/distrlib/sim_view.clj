(ns distrlib.sim-view
  (:require [yesql.core :refer (defqueries)]
            [seesaw.core :refer :all]
            [seesaw.tree :refer :all]
            [distrlib.sim :as sim]
            [clojure.java.jdbc :as jdbc]))

(defn list-from-db
  []
  (let [config {:store {:file "broadcast.db" :overwrite true}
                :event-schema {:lol :kw}
                :state-schema {:count :int}}]
    (sim/get-all-events 
      (sim/sqlite-db-spec
        (get-in config [:store :file]))
      (get config :event-schema)
      (get config :state-schema))))

; Render thing with right names and system icons
(defn render-event
  [time-format renderer {:keys [value]}]
  (config! renderer :text (str "["
                               (get-in value
                                       [:sender time-format])
                               "] "
                               (get-in value
                                       [:sender :node])
                               " -> "
                               (get value :dest))))

(defn properties-table-model
  "Given a map, returns a table model for displaying them"
  [m]
  [:columns [{:key :k :text "Key"}
             {:key :v :text "Value"}]
   :rows (mapv (fn [[k v]]
                 {:k k :v v})
               m)])

(defn details-panel
  "Takes a map from names of objects to their map value and
   returns a swing component to display them"
  [mapping]
  (vertical-panel
    :items (mapcat (fn [[l obj]]
                     [(label :text l)
                      (table :model
                             (properties-table-model obj))])
                   mapping)))

(defn make-frame
  [time-display-bg]
  (frame
    :title "Event Explorer" :width 500 :height 500 
    :content
    (border-panel
      :border 5 :hgap 5 :vgap 5
      :north (flow-panel
               :hgap 5 :vgap 5 :items
               [(radio :id :step-radio :text "Step #" :group time-display-bg
                       :selected? true)
                (radio :id :time-radio :text "Time" :group time-display-bg
                      ; :enabled? false
                       )])

        :center (left-right-split
                  (scrollable (listbox
                                :id :event-list
                                :model (list-from-db)
                                :renderer (partial render-event :step)))
                  (scrollable (vertical-panel :id :details-panel))
                  :divider-location 1/3)

        :south  (label :id :status :text "Ready"))))

(defn display-details
  "Event listener for rendering new details"
  [f e]
  (when-let [event (selection e)]
    (config! (select f [:#details-panel])
             :items [(details-panel
                       [["Event" (dissoc event
                                         :sender
                                         :reciever
                                         :sender-logs
                                         :reciever-logs)]
                        ["Sender" (:sender event)]
                        ["Reciever" (:reciever event)]
                        ["Sender Logs" (zipmap (range)
                                               (:sender-logs event))]
                        ["Reciever Logs" (zipmap (range)
                                                 (:reciever-logs event))]])])))

(defn example []
  (let [time-display-bg (button-group) 
        f (make-frame time-display-bg)]
    ; Hook up a selection listener to the tree to update stuff
    (listen (select f [:#event-list]) :selection
            (fn [e] (display-details f e)))
    (listen time-display-bg :selection
            (fn [e]
              (when (selection e)
                (cond
                  (config (select f [:#step-radio]) :selected?)
                  (config!
                    (select f [:#event-list])
                    :renderer (partial render-event :step))
                  (config (select f [:#time-radio]) :selected?)
                  (config!
                    (select f [:#event-list])
                    :renderer (partial render-event :time))))))
    (config! f :on-close :dispose)
    (pack! f)
    (show! f)))

;(run :dispose)

(comment
  (example)
  )
