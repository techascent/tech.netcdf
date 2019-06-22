(ns tech.netcdf
  (:require [tech.v2.datatype :as dtype]
            [tech.resource :as resource]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [tech.libs.netcdf :as lib-netcdf]
            [tech.v2.tensor :as dtt]
            [tech.v2.tensor.typecast :as dtt-typecast]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.boolean-op :as boolean-op])
  (:import [ucar.nc2.dataset NetcdfDataset]
           [ucar.nc2 Dimension NetcdfFile Attribute Variable]
           [ucar.unidata.geoloc ProjectionImpl LatLonRect]
           [ucar.nc2.dt.grid GridDataset GridDataset$Gridset]
           [ucar.nc2.dt GridDatatype GridCoordSystem]
           [ucar.ma2 DataType]
           [tech.v2.tensor FloatTensorReader]
           ))

(set! *warn-on-reflection* true)

;; Text format is cdl.  NetCDF doesn't know how to deal with that.
(def test-cdl-file "test/data/sresa1b_ncar_ccsm3-example.cdl")
;; Binary formats are .nc or .nc3, etc.  These we can open.
(def test-nc-file "test/data/sresa1b_ncar_ccsm3-example.nc")


(defn fname->netcdf
  "Open a netcdf file.  The return value is tracked via 'tech.resource/track.  If the
  user wishes to release the memory early they can call .close on the object."
  ^NetcdfFile [fname]
  ;;Files are Closeable and AutoCloseable so the resource system knows how to
  ;;deal with them.
  (-> (NetcdfDataset/openFile fname, nil)
      resource/track))


(defn dim->clj
  [^Dimension dim]
  {:name (.getName dim)
   :length (.getLength dim)
   :shared? (.isShared dim)
   :unlimited? (.isUnlimited dim)
   :variable-length? (.isVariableLength dim)})


(defn dimensions
  [^NetcdfFile netcdf]
  (->> (.getDimensions netcdf)
       (map dim->clj)))


(defn att->clj
  [^Attribute att]
  {:name (.getName att)
   :datatype (-> (.getDataType att)
                 lib-netcdf/netcdf-datatype-map)
   :is-array? (.isArray att)
   :is-string? (.isString att)
   :value (when-not (.isArray att)
            (if (.isString att)
              (.getStringValue att)
              (.getNumericValue att)))})

(defn named-item->map
  [named-item-seq]
  (->> named-item-seq
       (map (juxt :name identity))
       (into {})))


(defn attributes
  [^NetcdfFile netcdf]
  (->> (.getGlobalAttributes netcdf)
       (map att->clj)
       named-item->map))


(defn var->clj
  [^Variable variable]
  {:variable variable
   :name (.getName variable)
   :dimensions (->> (.getDimensions variable)
                    (map dim->clj))
   :attributes (->> (.getAttributes variable)
                    (map att->clj)
                    named-item->map)
   :datatype (-> (.getDataType variable)
                 lib-netcdf/netcdf-datatype-map)
   :shape (vec (.getShape variable))
   ;;Maybe you don't need the data right away
   :data (delay (.read variable))})


(defn variables
  [^NetcdfFile netcdf]
  (->> (.getVariables netcdf)
       (map var->clj)
       named-item->map))


(defn overview
  [netcdf]
  (->> (variables netcdf)
       vals
       (map (fn [{:keys [attributes dimensions] :as vardata}]
              (assoc (select-keys vardata [:name :shape :data])
                     :shape-names (mapv :name dimensions)
                     :fullname (get-in attributes ["standard_name" :value]
                                       (:name vardata))
                     :units (get-in attributes ["units" :value])
                     :missing-value (get-in attributes ["missing_value" :value]))))
       named-item->map))


(defn netcdf->gridsets
  "Return a sequence of gridsets.  A gridset has a coordinate-space and a sequence
  of grids.  Grids have a name, attributes, and a 2d tensor.  When creating the gridset,
  you can choose the time-axis and the z-axis to choose.  -1 means choose the entire
  dimension; defaults to 0 meaning all of the returned grids is 2 dimension.  It should
  be safe to close the netcdf file after this operation."
  [netcdf & {:keys [time-axis z-axis fill-value]
             :or {time-axis 0 z-axis 0}}]
  (let [netcdf (cond
                 (instance? NetcdfDataset netcdf)
                 netcdf
                 (instance? NetcdfFile netcdf)
                 (resource/track (NetcdfDataset. ^NetcdfFile netcdf true))
                 :else
                 (throw (ex-info "Failed to transform input to netcdf dataset" {})))
        grid-ds ^GridDataset (resource/track (GridDataset. netcdf))
        grids (.getGridsets grid-ds)]
    (->> grids
         (mapv (fn [^GridDataset$Gridset gridset]
                 (let [grid-data
                       (->> (.getGrids gridset)
                            (map (fn [^GridDatatype %]
                                   (let [atts (->> (.getAttributes %)
                                                   (map att->clj)
                                                   named-item->map)
                                         missing-value (-> (get-in atts
                                                                   ["missing_value"
                                                                    :value])
                                                           float)
                                         data (-> (.readDataSlice % time-axis
                                                                  z-axis
                                                                  -1 -1)
                                                  dtt/ensure-tensor)
                                         data (if (and missing-value fill-value)
                                                (unary-op/unary-reader
                                                 :float32
                                                 (if (= 0 (Float/compare
                                                           x missing-value))
                                                   fill-value
                                                   x)
                                                 data)
                                                data)]
                                     {:name (.getName %)
                                      :attributes atts
                                      ;;force to float32 regardless
                                      :data (dtt/clone data :datatype :float32
                                                       :container-type
                                                       :native-buffer)}))))]
                   {:coordinate-system (.getGeoCoordSystem gridset)
                    :projection (.getProjection (.getGeoCoordSystem gridset))
                    :grids grid-data}))))))


(defn lat-lon-query-gridsets
  "Ignoring time, query a dataset by lat-lon.  Example output:

  ({:lat 21.145,
  :lng 237.307,
  :grid-data
  [{:missing-value ##NaN,
    :fullname \"Temperature_surface\",
    :abbreviation \"TMP\",
    :level-desc \"Ground or water surface\",
    :level-type 1,
    :values
    ({:cell-lat 21.14511133620467,
      :cell-lng 237.30713946785917,
      :row 0,
      :col 1,
      :data 296.22930908203125}
      ...)
    ...])"
  [gridsets lat-lon-seq & {:keys [query-gridsize]
                           :or {query-gridsize 3}}]
  (->> gridsets
       (mapcat
        (fn [{:keys [coordinate-system
                     projection
                     grids]}]
          (let [lat-ary (float-array (map first lat-lon-seq))
                lon-ary (float-array (map second lat-lon-seq))
                ^GridCoordSystem coordinate-system coordinate-system
                n-elems (count lat-lon-seq)
                n-x (-> (.getXHorizAxis coordinate-system)
                        (.getShape)
                        first
                        long)
                n-y (-> (.getYHorizAxis coordinate-system)
                        (.getShape)
                        first
                        long)
                grid-inc (quot (long query-gridsize) 2)]
            (->>
             (range n-elems)
             (map
              (fn [idx]
                (let [lat (aget lat-ary idx)
                      lng (aget lon-ary idx)
                      item-idx (.findXYindexFromLatLon coordinate-system
                                                       lat lng
                                                       (int-array 2))
                      x-idx (aget item-idx 0)
                      y-idx (aget item-idx 1)
                      x-idx-range (range (max 0 (- x-idx grid-inc))
                                         (min n-x (+ x-idx grid-inc 1)))
                      y-idx-range (range (max 0 (- y-idx grid-inc))
                                         (min n-y (+ y-idx grid-inc 1)))]
                  {:lat lat
                   :lng lng
                   :row y-idx
                   :col x-idx
                   :x-idx-range x-idx-range
                   :y-idx-range y-idx-range
                   :grid-latlngs (-> (for [y-idx y-idx-range
                                           x-idx x-idx-range]
                                       (let [ll (.getLatLon coordinate-system
                                                            x-idx
                                                            y-idx)
                                             lng (.getLongitude ll)]
                                         [(.getLatitude ll)
                                          (if (< lng 0)
                                            (+ lng 360.0)
                                            lng)]))
                                     (dtt/->tensor)
                                     (dtt/reshape [(count y-idx-range)
                                                   (count x-idx-range)
                                                   2]))
                   :grid-data
                   (->> grids
                        (mapv
                         (fn [{:keys [name attributes data]}]
                           {:missing-value (get-in attributes
                                                   ["missing_value" :value])
                            :fullname name
                            :abbreviation (get-in attributes
                                                  ["abbreviation" :value])
                            :level-desc (get-in attributes
                                                ["Grib2_Level_Desc" :value])
                            :level-type (get-in attributes
                                                ["Grib2_Level_Type" :value])
                            ;;clone to make repl life better
                            :values (dtt/select data y-idx-range x-idx-range)}
                           )))})))))))))


(defn distance-lerp-lat-lon-query
  "Return weighted average of all values by distance.  If options contains
  :fill-value and one of the values is equal to the entry missing value, fill-value
  will be used instead.  If fill value is not provided, then 0 is used.  This will
  lower your average temp.

  tech.netcdf> (distance-lerp-lat-lon-query (first query))
{:lat 21.145
 :lng 237.307
 :grid-data
 [{:missing-value ##NaN
   :fullname \"Temperature_surface\"
   :abbreviation \"TMP\"
   :level-desc \"Ground or water surface\"
   :level-type 1
   :value 296.20944}]}"
  [{:keys [lat lng grid-latlngs grid-data] :as query-data}
   & [options]]
  (let [target-latlngs (float-array [(:lat query-data)
                                     (:lng query-data)])
        [n-y n-x _] (dtype/shape grid-latlngs)
        weights (mapv (partial dfn/distance target-latlngs)
                      (dtt/slice grid-latlngs 2))
        weights (dfn/- 1 (dfn// weights (dfn/+ weights)))
        ;;normalize weights
        weights (dfn// weights (dfn/+ weights))]
    (-> query-data
        (dissoc :grid-latlngs :x-idx-range :y-idx-range
                :row :col)
        (assoc :grid-data
               (->> grid-data
                    (mapv (fn [{:keys [missing-value values] :as entry}]
                            ;;flatten values out.
                            (let [missing-value (float missing-value)]
                              (-> (dissoc entry :values)
                                  (assoc :value
                                         (->> (binary-op/binary-reader
                                               :float32
                                               (if (= 0
                                                      (Float/compare
                                                       x
                                                       (float missing-value)))
                                                 0
                                                 (* x y))
                                               values
                                               weights)
                                              dfn/+)))))))))))


(defn exact-match-lat-lon-query
  "Return the item that was the closest match to the distance query.
  If fill-value is provided in options then it will be used should
  the exact match be a missing value.  Else the missing value will
  be returned.
tech.netcdf> (exact-match-lat-lon-query (first query))
{:lat 21.145
 :lng 237.307
 :query-lat 21.14511133620467
 :query-lng 237.30713946785917
 :grid-data
 [{:missing-value ##NaN
   :fullname \"Temperature_surface\"
   :abbreviation \"TMP\"
   :level-desc \"Ground or water surface\"
   :level-type 1
   :value 296.2293}]}"
  [{:keys [lat lng row col x-idx-range y-idx-range grid-latlngs
           grid-data] :as query-data}
   & [options]]
  (let [rel-row (first (dfn/argfilter
                        (boolean-op/make-boolean-unary-op :object (= row x))
                        y-idx-range))
        rel-col (first (dfn/argfilter
                        (boolean-op/make-boolean-unary-op :object (= col x))
                        x-idx-range))
        [query-lat query-lng] (-> (dtt/select grid-latlngs rel-row rel-col (range 2))
                                  (dtype/->reader))]
    (-> query-data
        (dissoc :grid-latlngs :x-idx-range :y-idx-range
                :row :col)
        (assoc :query-lat query-lat
               :query-lng query-lng
               :grid-data
               (->> grid-data
                    (mapv (fn [{:keys [values missing-value] :as entry}]
                            (-> entry
                                (dissoc :values)
                                (assoc :value
                                       (let [val
                                             (.read2d (dtt-typecast/->float32-reader
                                                       values
                                                       false)
                                                      rel-row rel-col)]
                                         (if (and (= 0 (Float/compare
                                                        val
                                                        missing-value))
                                                  (:fill-value options))
                                           (:fill-value options)
                                           val)))))))))))
