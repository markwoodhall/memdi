(ns memdi.node)

(defprotocol Node
  "A protocol representing a memdi node."
  (write-key [this k v])
  (read-key [this k]))

(defprotocol MasterNode
  "A protocol representing a memdi master node."
  (add-slave [this slave]))

(defprotocol SlaveNode
  "A protocol representing a memdi slave node."
  (replicate-key [this k v]))

(defrecord InMemoryMasterNode [store name slaves])
(defrecord InMemorySlaveNode [store name master])

(extend-type InMemoryMasterNode Node
  (write-key
    [this k v]
    (-> (:store this)
        (swap! conj {(keyword k) v}))
    (let [slaves @(:slaves this)]
        (doseq [slave slaves]
          (replicate-key slave k v)))
    this)
  (read-key
    [this k]
    (-> @(:store this)
        ((keyword k)))))

(extend-type InMemoryMasterNode MasterNode
  (add-slave
    [this slave]
    (-> (:slaves this)
        (swap! conj (assoc slave :master this)))
    (-> (:master slave)
        (reset! this))
    (let [store @(:store this)]
      (doseq [[k v] store]
        (replicate-key slave (name k) v)))
    this))

(extend-type InMemorySlaveNode Node
  (write-key
    [this k v]
    (-> @(:master this)
        (write-key k v)))
  (read-key
    [this k]
    (-> @(:store this)
        ((keyword k)))))

(extend-type InMemorySlaveNode SlaveNode
  (replicate-key
    [this k v]
    (-> (:store this)
        (swap! conj {(keyword k) v}))))

(defn master-node
  "Given a name return a new memdi node with
  no slaves."
  [name]
   (InMemoryMasterNode. (atom {}) name (atom [])))

(defn slave-node
  "Given a name return a new memdi node with
  the specified master."
  [name]
  (InMemorySlaveNode. (atom {}) name (atom {})))
