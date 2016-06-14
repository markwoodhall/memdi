(ns memdi.node-test
  (:require [clojure.test :refer [deftest testing is]]
            [memdi.node :refer [master-node Node write-key read-key]]))

(deftest master-node-test
  (testing "A master node should be created with the correct values"
    (let [expected {:master? true
                    :name "daredevil"}]
      (is (.equals expected (-> (master-node "daredevil")
                                (dissoc :store))))))

  (testing "A master node should accept a write and store the key/value,
           it should be retrievable immediately."
    (let [node (master-node "daredevil")]
      (write-key node "key" "String data")
      (is (= (read-key node "key") "String data")))))
