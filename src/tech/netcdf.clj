(ns tech.netcdf
  (:require [tech.v2.datatype :as dtype]
            [tech.resource :as resource]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [tech.libs.netcdf :as lib-netcdf]
            [tech.v2.tensor :as dtt]
            [tech.v2.tensor.typecast :as dtt-typecast]
            [tech.v2.datatype.typecast :as dtype-typecast]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.boolean-op :as boolean-op])
  (:import [ucar.nc2.dataset NetcdfDataset CoordinateAxis1D]
           [ucar.nc2 Dimension NetcdfFile Attribute Variable]
           [ucar.unidata.geoloc ProjectionImpl LatLonRect]
           [ucar.nc2.dt.grid GridDataset GridDataset$Gridset]
           [ucar.nc2.dt GridDatatype GridCoordSystem]
           [ucar.ma2 DataType]
           [tech.v2.tensor FloatTensorReader]
           [tech.v2.datatype IntReader FloatReader]
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


(defn fname->grids
  "Return a sequence of grids.  A grid is a coordinate space and cube of data.
  Grids have a name, attributes, and a 2d tensor.  When creating the grids,
  you can choose the time-axis and the z-axis to choose.  -1 means choose the entire
  dimension; defaults to 0 meaning all of the returned grids is 2 dimension.  Code
  is written with the assumtion that a non-negative integer will be chosen for
  the time-axis and the z-axis as this will increase the dimensionality of the data
  tensor.  Returns a sequence of grids where a grid is:
  {:name
   :attributes map of name->attribute
   :coordinate-system GridCoordSystem
   :data tensor
   :data-reader FloatTensorReader (allows arbitrary indexing and ad-hoc algorithms)
  }
  "
  [fname & {:keys [time-axis z-axis fill-value datatype]
            :or {time-axis 0 z-axis 0 datatype :float32}}]
  (resource/stack-resource-context
   (let [netcdf (fname->netcdf fname)
         ;;True stands for 'enhanced'.
         ;;https://www.unidata.ucar.edu/software/thredds/current/netcdf-java/tutorial/NetcdfDataset.html
         dataset (resource/track (NetcdfDataset. netcdf true))
         grid-ds ^GridDataset (resource/track (GridDataset. dataset))]
      (->> (.getGridsets grid-ds)
           (mapcat
            (fn [^GridDataset$Gridset gridset]
              (->> (.getGrids gridset)
                   (map
                    (fn [^GridDatatype %]
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
                            data (-> (if (and missing-value fill-value)
                                       (unary-op/unary-reader
                                        :float32
                                        (if (= 0 (Float/compare
                                                  x missing-value))
                                          fill-value
                                          x)
                                        data)
                                       data)
                                     ;; If you want to use TVM or numpy, choose a native
                                     ;; buffer tensor.
                                     (dtt/clone
                                      :datatype datatype
                                      :container-type :native-buffer))]
                        {:name (.getName %)
                         :attributes atts
                         :data data
                         :data-reader (dtt-typecast/->float32-reader
                                       data
                                       false)
                         :coordinate-system (.getGeoCoordSystem gridset)}))))))
           vec))))


(def test-file "test/data/3600.grib2")
(def test-lat-lng [[21.145 237.307]])


(defn lat-lng-query-grid-exact
  "Ignoring time, query a grid pointwise by lat-lon.  Returns only closest match.
  Takes a grid and a sequence of [lat lng] tuples.
  Returns
  {
   :missing-value ##NaN,
   :fullname \"Temperature_surface\",
   :abbreviation \"TMP\",
   :level-desc \"Ground or water surface\",
   :level-type 1,
   :cell-lat-lngs - tensor<float32>[n-elems 2] - lat-lng tensor of cell centers.
   :values - float32[n-elems]
  }"
  [{:keys [coordinate-system name attributes data-reader] :as grid}
   lat-lon-seq & [options]]
  (let [^java.util.List lat-lng-seq (vec lat-lon-seq)
        n-elems (count lat-lng-seq)
        ^GridCoordSystem coordinate-system coordinate-system
        ^CoordinateAxis1D x-axis (.getXHorizAxis coordinate-system)
        ^CoordinateAxis1D y-axis (.getYHorizAxis coordinate-system)
        n-x (-> x-axis
                (.getShape)
                first
                long)
        n-y (-> y-axis
                (.getShape)
                first
                long)
        lat-ary (-> (reify FloatReader
                      (lsize [_] n-elems)
                      (read [_ idx] (first (.get lat-lng-seq idx))))
                    (dtype/copy! (float-array n-elems)))
        lng-ary (-> (reify FloatReader
                      (lsize [_] n-elems)
                      (read [_ idx] (second (.get lat-lng-seq idx))))
                    (dtype/copy! (float-array n-elems)))
        x-coords (float-array n-elems)
        y-coords (float-array n-elems)
        _ (-> (.getProjection coordinate-system)
              (.latLonToProj ^"[[F" (into-array [lat-ary lng-ary])
                             ^"[[F" (into-array [x-coords y-coords])
                             0 1))
        x-idx (reify IntReader
                (lsize [_] n-elems)
                (read [_ idx]
                  (.findCoordElementBounded x-axis (aget x-coords idx))))
        y-idx (reify IntReader
                (lsize [_] n-elems)
                (read [_ idx]
                  (.findCoordElementBounded y-axis (aget y-coords idx))))
        cell-lat-lngs (dtype/object-reader
                       n-elems
                       (fn [idx]
                         (let [ll (.getLatLon coordinate-system
                                              (.read x-idx idx)
                                              (.read y-idx idx))
                               lng (.getLongitude ll)]
                           [(.getLatitude ll)
                            (if (< lng 0)
                              (+ lng 360.0)
                              lng)])))
        ^FloatTensorReader data-reader data-reader
        values (-> (reify FloatReader
                     (lsize [_] n-elems)
                     (read [_ idx]
                       (.read2d data-reader (.read y-idx idx) (.read x-idx idx))))
                   (dtype/copy! (float-array n-elems)))]
    {:missing-value (get-in attributes ["missing_value" :value])
     :fullname name
     :abbreviation (get-in attributes ["abbreviation" :value])
     :level-desc (get-in attributes ["Grib2_Level_Desc" :value])
     :level-type (get-in attributes ["Grib2_Level_Type" :value])
     :cell-lat-lngs cell-lat-lngs
     :values values}))


(comment
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
                                                dfn/+))))))))))))
