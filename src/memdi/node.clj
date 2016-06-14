(ns memdi.node)

(defprotocol Node
  "A protocol representing a memdi node."
  (write-key [this k v])
  (read-key [this k]))

(defrecord SimpleNode [store name master?])

(extend-type SimpleNode Node
  (write-key
    [this k v]
    (-> (:store this)
        (swap! conj {(keyword k) v}))
    (:store this))
  (read-key
    [this k]
    (-> @(:store this)
        ((keyword k)))))

(defn- node
  [name master]
  (SimpleNode. (atom {}) name master))

(defn master-node
  "Given a name return a new memdi node with
  the specified name and `:master? true`"
  [name]
  (node name true))

(defn slave-node
  "Given a name return a new memdi node with
  the specified name and `:master? false`"
  [name]
  (node name false))


