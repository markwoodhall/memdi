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

(defn master-node
  "Given a name return a new memdi node with
  the specified name and `:master? true`"
  [name]
  (SimpleNode. (atom {}) name true))


