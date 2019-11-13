(ns tech.netcdf-test
  (:require [tech.netcdf :as netcdf]
            [tech.v2.tensor :as dtt]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.binary-op :as bin-op]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.readers.const :as const-rdr]
            [tech.resource :as resource]
            [clojure.edn :as edn]
            [clojure.test :refer [deftest is]])
  (:import [ucar.unidata.geoloc.projection LambertConformal]
           [ucar.unidata.geoloc LatLonPointImpl]))


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


(defn endless-lat-lng-stream
  []
  (->> (slurp "test/data/airport-summary.edn")
       edn/read-string
       repeat
       (map shuffle)
       (apply concat)
       (map (juxt :lat :lng))))


(deftest grib2-lat-lon-lookup
  (try
    (let [grids (netcdf/fname->grids "./test/data/3600.grib2")
          lookup (-> (netcdf/lat-lng-query-grid-exact (first grids)
                                                      [[21.145 237.307]
                                                       [21.145 (- 237.307
                                                                  360.0)]])
                     (dissoc :cell-lat-lngs :missing-value)
                     (update :values
                             (fn [values]
                               (->> values
                                    (map #(-> (* % 100)
                                              (Math/round)
                                              long))
                                    vec))))
          query-fn (netcdf/setup-nearest-projection (first grids))]
      (is (= {:fullname "Temperature_surface"
              :abbreviation "TMP"
              :level-desc "Ground or water surface"
              :level-type 1
              :values [29623 29623]}
             lookup))
      (is (= [29623 29623]
             (->> (query-fn [[21.145 237.307]
                             [21.145 (- 237.307
                                        360.0)]])
                  (mapv #(-> (* % 100)
                             (Math/round)
                             long)))))
      (let [lat-lng-data (take 100 (endless-lat-lng-stream))
            grid (first grids)
            exact-query (-> (netcdf/lat-lng-query-grid-exact grid
                                                             lat-lng-data)
                            :values)
            lininterp-query-fn (netcdf/setup-linear-interpolator
                                grid grid
                                (:* bin-op/builtin-binary-ops))
            precalc-query (query-fn lat-lng-data)
            lininterp-query (lininterp-query-fn lat-lng-data
                                                (const-rdr/make-const-reader 0.5
                                                                             :float32)
                                                (const-rdr/make-const-reader 0.5
                                                                             :float32))]
        (is (dfn/equals exact-query precalc-query))
        (is (dfn/equals exact-query lininterp-query))))
    (catch java.io.FileNotFoundException e
      (println "File not found, maybe try: ./scripts/get_test_data.sh")
      (throw e))))


(deftest linear-interpolator-test
  (let [grid (first (netcdf/fname->grids "./test/data/3600.grib2"))
        interpolator (netcdf/setup-linear-interpolator grid grid
                                                       (:* bin-op/builtin-binary-ops))
        values (->> (interpolator [[21.145 237.307]
                                   [21.145 (- 237.307
                                              360.0)]]
                                  (const-rdr/make-const-reader 0.5
                                                               :float32)
                                  (const-rdr/make-const-reader 0.5
                                                               :float32))
                    (map #(-> (* % 100)
                              (Math/round)
                              long))
                    vec)]
    (is (= [29623 29623] values))))


(deftest grib2-time-test
  (println "base lookup method")
  (let [grids (netcdf/fname->grids "./test/data/3600.grib2")
        data (take 100000 (endless-lat-lng-stream))]
    (dotimes [iter 5]
      ;;Make sure everything is totally realized.
      (time (-> (netcdf/lat-lng-query-grid-exact (first grids) data)
                :values
                count)))))


(deftest prebaked-time-test
  (println "prebaked")
  (let [grids (netcdf/fname->grids "./test/data/3600.grib2")
        data (vec (take 100000 (endless-lat-lng-stream)))
        query-fn (netcdf/setup-nearest-projection (first grids))]
    (dotimes [iter 5]
      ;;Make sure everything is totally realized.
      (time (-> (query-fn data)
                (dtype/copy! (double-array (count data))))))))


(deftest lerp-time-test
  (println "prebake-lerp")
  (let [grids (netcdf/fname->grids "./test/data/3600.grib2")
        data (vec (take 100000 (endless-lat-lng-stream)))
        query-fn (netcdf/setup-linear-interpolator
                  (first grids)
                  (first grids))]
    (dotimes [iter 5]
      ;;Make sure everything is totally realized.
      (time (-> (query-fn data
                          (const-rdr/make-const-reader 0.5
                                                       :float32)
                          (const-rdr/make-const-reader 0.5
                                                       :float32))
                (dtype/copy! (float-array (count data))))))))
