-- name: lookup-node-name->id*
-- Given a node name, finds its id
SELECT rowid FROM node_names WHERE name = :nodename

-- name: lookup-node-id->name*
-- Given a node id, finds its name
SELECT name FROM node_names WHERE rowid = :nodeid

-- name: get-all-states*
-- Returns all the state data, joined with node names
SELECT *
FROM primary_state, node_names
WHERE node_names.rowid = primary_state.node
ORDER BY primary_state.step

-- name: get-all-events*
-- Returns all the event data, joined with state data & names
-- TODO make this be a few queries driven by the client
SELECT * FROM primary_events

-- name: latest-state-id-for-node*
-- Finds the rowid of the most recent state for a node as of a given time
SELECT primary_state.rowid
FROM primary_state, node_names
WHERE time = (SELECT max(time)
    FROM primary_state, node_names
    WHERE primary_state.node = node_names.rowid
    AND node_names.name = :nodename
    AND primary_state.time <= :time)
AND primary_state.node = node_names.rowid
AND node_names.name = :nodename

-- name: get-state-by-state-id
-- returns the state for the given state id
SELECT *
FROM primary_state
WHERE primary_state.rowid = :id

-- name: add-reciever-to-event!
-- Updates the given event with the target reciever
UPDATE primary_events
SET reciever_state = :reciever_state_id
WHERE rowid = :id

-- name: get-logs
-- Retrieves all the logs by node & time
SELECT msg
FROM user_logs
WHERE user_logs.time = :time
AND user_logs.node = :node
