(ns memdi.node)

(defprotocol Node
  "A protocol representing a memdi node."
  (write [this k v])
  (read [this k]))

(defprotocol MasterNode
  "A protocol representing a memdi master node."
  (add-slave [this slave]))

(defprotocol ReadWrite
  "A protocol to represent a read write strategy."
  (write-key [this k v])
  (read-key [this k])
  (replicate-key [this k v]))

(defrecord Master [rw-strategy name slaves])
(defrecord Slave [rw-strategy name master])
(defrecord InMemoryReadWrite [store])

(extend-type InMemoryReadWrite ReadWrite
  (write-key
    [this k v]
    (-> (:store this)
        (swap! conj {(keyword k) v})))
  (read-key
    [this k]
    (-> @(:store this)
        ((keyword k))))
  (replicate-key
    [this k v]
    (-> (:store this)
        (swap! conj {(keyword k) v}))))

(extend-type Master Node
  (write
    [this k v]
    (-> (:rw-strategy this)
        (write-key k v))
    (let [slaves @(:slaves this)]
        (doseq [{:keys [rw-strategy]} slaves]
          (replicate-key rw-strategy k v)))
    this)
  (read
    [this k]
    (-> (:rw-strategy this)
        (read-key (keyword k)))))

(extend-type Master MasterNode
  (add-slave
    [this slave]
    (-> (:slaves this)
        (swap! conj (assoc slave :master this)))
    (-> (:master slave)
        (reset! this))
    (let [store @(:store (:rw-strategy this))]
      (doseq [[k v] store]
        (replicate-key (:rw-strategy slave) (name k) v)))
    this))

(extend-type Slave Node
  (write
    [this k v]
    (-> @(:master this)
        (:rw-strategy)
        (write-key k v)))
  (read
    [this k]
    (-> (:rw-strategy this)
        (read-key (keyword k)))))

(defn master-node
  "Given a name return a new memdi node with
  no slaves."
  [name]
   (Master. (InMemoryReadWrite.  (atom {})) name (atom [])))

(defn slave-node
  "Given a name return a new memdi node with
  the specified master."
  [name]
  (Slave. (InMemoryReadWrite. (atom {})) name (atom {})))
