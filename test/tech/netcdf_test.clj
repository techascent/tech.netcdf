(ns tech.netcdf-test
  (:require [tech.netcdf :as netcdf]
            [tech.v2.tensor :as tens]
            [tech.resource :as resource]
            [clojure.test :refer [deftest is]]))


(deftest test-netcdf-functionality
  (resource/stack-resource-context
   (try
     (let [test-file (netcdf/fname->netcdf netcdf/test-nc-file)
           overview (netcdf/overview test-file)]
       (is (= 12 (count overview)))
       (is (= #{"tas" "ua" "area" "lon" "time" "lat" "time_bnds" "msk_rgn"
                "lat_bnds" "lon_bnds" "plev" "pr"}
              (set (keys overview))))

       (let [fdata @(get-in overview ["tas" :data])]
         (is (= [[215 215 215] [217 217 216] [218 218 218]]
                (-> (tens/select fdata 0 (range 3) (range 3))
                    ;; set the type to something we can test against
                    (tens/clone :datatype :int32)
                    (tens/->jvm))))))
     (catch java.io.FileNotFoundException e
       (println "File not found, maybe try: ./scripts/get_test_data.sh")
       (throw e)))))

(deftest test-grib2
  (resource/stack-resource-context
   (try
     (let [ds (netcdf/fname->netcdf "./test/data/3600.grib2")
           overview (netcdf/overview ds)]
       (is (= (set (keys overview))
              #{"LambertConformal_Projection" "x" "y" "reftime" "time"
                "Temperature_surface"})))
     (catch java.io.FileNotFoundException e
       (println "File not found, maybe try: ./scripts/get_test_data.sh")
       (throw e)))))


(deftest grib2-lat-lon-lookup
  (resource/stack-resource-context
   (try
     (let [ds (netcdf/fname->netcdf "./test/data/3600.grib2")
           gs (netcdf/netcdf->gridsets ds)
           lookup (netcdf/lat-lon-query-gridsets gs [[21.145 237.307]])]
       (is (= 1 (count lookup)))
       (let [gd (-> (first lookup)
                    :grid-data)]
         (is (= 1 (count gd)))
         (let [gd (first gd)]
           (is (= 4 (count (:values gd))))
           (is (= [29622 29616 29622 29622]
                  (mapv (comp long (partial * 100) :data) (:values gd)))))))
     (catch java.io.FileNotFoundException e
       (println "File not found, maybe try: ./scripts/get_test_data.sh")
       (throw e))))
  )
