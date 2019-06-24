(ns tech.netcdf-test
  (:require [tech.netcdf :as netcdf]
            [tech.v2.tensor :as dtt]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype :as dtype]
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
                (-> (dtt/select fdata 0 (range 3) (range 3))
                    ;; set the type to something we can test against
                    (dtt/clone :datatype :int32)
                    (dtt/->jvm))))))
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
  (try
    (let [grids (netcdf/fname->grids "./test/data/3600.grib2")
          lookup (-> (netcdf/lat-lng-query-grid-exact (first grids) [[21.145 237.307]
                                                                     [21.145 237.307]])
                     (dissoc :cell-lat-lngs :missing-value)
                     (update :values
                             (fn [values]
                               (->> values
                                    (map #(-> (* % 100)
                                              (Math/round)
                                              long))
                                    vec))))]
      (is (= {:fullname "Temperature_surface"
              :abbreviation "TMP"
              :level-desc "Ground or water surface"
              :level-type 1
              :values [29623 29623]}
             lookup)))
    (catch java.io.FileNotFoundException e
      (println "File not found, maybe try: ./scripts/get_test_data.sh")
      (throw e)))
  )
