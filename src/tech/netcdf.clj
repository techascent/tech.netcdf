(ns tech.netcdf
  (:require [tech.v2.datatype :as dtype]
            [tech.resource :as resource]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [tech.libs.netcdf :as lib-netcdf]
            [tech.v2.tensor :as tens])
  (:import [ucar.nc2.dataset NetcdfDataset]
           [ucar.nc2 Dimension NetcdfFile Attribute Variable]
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
