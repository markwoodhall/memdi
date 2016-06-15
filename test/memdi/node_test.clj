(ns memdi.node-test
  (:require [clojure.test :refer [deftest testing is]]
            [memdi.node :refer [master-node slave-node write-key read-key add-slave]]))

(deftest master-node-test
  (testing "A master node should be created with the correct values"
    (let [expected {:name "daredevil"}]
      (is (.equals expected (-> (master-node "daredevil")
                                (dissoc :store)
                                (dissoc :slaves))))))

  (testing "A master node should accept a write and store the key/value,
           it should be retrievable immediately."
    (let [node (master-node "daredevil")]
      (write-key node "key" "String data")
      (is (= (read-key node "key") "String data")))))

(deftest slave-node-test
  (testing "A slave node should be created with the correct values"
    (let [master (master-node "daredevil")
          slave (slave-node "foggy")
          slave (first @(:slaves (add-slave master slave)))
          expected {:name "foggy"
                    :master master}]
      (is (.equals expected (-> slave
                                (dissoc :store))))))

  (testing "A slave node should accept a write on masters behalf"
    (let [slave (slave-node "foggy")
          master (master-node "daredevil")]
      (add-slave master slave)
      (write-key slave "key" "String data")
      (is (= (read-key master "key") "String data")))))
