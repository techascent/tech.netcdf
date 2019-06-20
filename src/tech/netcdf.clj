(ns tech.netcdf
  (:require [tech.v2.datatype :as dtype]
            [tech.resource :as resource]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [tech.libs.netcdf :as lib-netcdf]
            [tech.v2.tensor :as tens])
  (:import [ucar.nc2.dataset NetcdfDataset]
           [ucar.nc2 Dimension NetcdfFile Attribute Variable]
           [ucar.unidata.geoloc ProjectionImpl LatLonRect]
           [ucar.nc2.dt.grid GridDataset GridDataset$Gridset]
           [ucar.nc2.dt GridDatatype GridCoordSystem]
           [ucar.ma2 DataType]))

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
  [netcdf]
  (let [netcdf (cond
                 (instance? NetcdfDataset netcdf)
                 netcdf
                 (instance? NetcdfFile netcdf)
                 (NetcdfDataset. ^NetcdfFile netcdf true)
                 :else
                 (throw (ex-info "Failed to transform input to netcdf dataset" {})))
        grid-ds (GridDataset. netcdf)
        grids (.getGridsets grid-ds)]
    (->> grids
         (mapv (fn [^GridDataset$Gridset gridset]
                 (let [grid-data (->> (.getGrids gridset)
                                      (map (fn [^GridDatatype %]
                                             (let [atts (->> (.getAttributes %))])
                                             {:name (.getName %)
                                              :attributes (->> (.getAttributes %)
                                                               (map att->clj)
                                                               named-item->map)
                                              :data %})))]
                   {:coordinate-system (.getGeoCoordSystem gridset)
                    :projection (.getProjection (.getGeoCoordSystem gridset))
                    :grids grid-data}))))))


(defn lat-lon-query-gridsets
  "Ignoring time, query a dataset by lat-lon"
  [gridsets lat-lon-seq & {:keys [time-axis z-axis]
                           :or {time-axis 0 z-axis 0}}]
  (->> gridsets
       (mapcat
        (fn [{:keys [coordinate-system
                     projection
                     grids]}]
          (let [lat-ary (float-array (map first lat-lon-seq))
                lon-ary (float-array (map second lat-lon-seq))
                ^GridCoordSystem coordinate-system coordinate-system
                n-elems (count lat-lon-seq)]
            (->>
             (range n-elems)
             (pmap
              (fn [idx]
                (let [lat (aget lat-ary idx)
                      lng (aget lon-ary idx)
                      item-idx (.findXYindexFromLatLon coordinate-system
                                                       lat lng
                                                       (int-array 2))
                      x-idx (aget item-idx 0)
                      y-idx (aget item-idx 1)]
                  {:lat lat
                   :lng lng
                   :grid-data
                   (->> grids
                        (mapv (fn [{:keys [name attributes data]}]
                                (let [^GridDatatype grid data
                                      [n-y n-x] (take-last 2 (.getShape grid))
                                      x-inc-idx (min (- n-x 1) (inc x-idx))
                                      y-inc-idx (min (- n-y 1) (inc y-idx))]
                                  {:missing-value (get-in attributes
                                                          ["missing_value" :value])
                                   :fullname (.getName grid)
                                   :abbreviation (get-in attributes
                                                         ["abbreviation" :value])
                                   :level-desc (get-in attributes
                                                       ["Grib2_Level_Desc" :value])
                                   :level-type (get-in attributes
                                                       ["Grib2_Level_Type" :value])
                                   :values (for [x-idx [x-idx x-inc-idx]
                                                 y-idx [y-idx y-inc-idx]]
                                             {:row y-idx
                                              :col x-idx
                                              :data (-> (.readDataSlice grid
                                                                        time-axis
                                                                        z-axis
                                                                        y-idx
                                                                        x-idx)
                                                        (.getDouble 0))})}))))})))))))))
