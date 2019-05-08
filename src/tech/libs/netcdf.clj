(ns tech.libs.netcdf
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [tech.jna :as jna]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.writer :as writer]
            [tech.v2.datatype.base :as dtype-base])
  (:import [ucar.ma2 Array DataType]
           [tech.v2.datatype
            BooleanReader ObjectReader
            BooleanWriter ObjectWriter]))

(set! *warn-on-reflection* true)


(def keyword-remap
  {:byte :int8
   :short :int16
   :int :int32
   :long :int64
   :float :float32
   :double :float64
   :boolean :boolean
   :string :string})

(defn enum-values->map
  [enum-values]
  (->> enum-values
       (map (fn [^Enum item]
              [item (-> (.toString item)
                        ->kebab-case
                        keyword)]))
       (into {})))


(def netcdf-datatype-map (enum-values->map (DataType/values)))


(defn netcdf-datatype->datatype
  [netcdf-datatype]
  (get keyword-remap netcdf-datatype :object))


(extend-type Array
  dtype-proto/PDatatype
  (get-datatype [item] (-> (.getDataType item)
                           netcdf-datatype-map
                           netcdf-datatype->datatype))

  dtype-proto/PCountable
  (ecount [item] (.getSize item))

  dtype-proto/PShape
  (shape [m] (vec (.getShape m)))

  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (-> (dtype-proto/get-datatype item)
        casting/numeric-type?))
  (->buffer-backing-store [item]
    (dtype-proto/->buffer-backing-store (.getStorage item)))


  dtype-proto/PToList
  (convertible-to-fastutil-list? [item] true)
  (->list-backing-store [item]
    (dtype-proto/->list-backing-store (.getStorage item)))


  dtype-proto/PToArray
  (->sub-array [item]
    (dtype-proto/->sub-array (.getStorage item)))
  (->array-copy [item]
    (dtype-proto/->array-copy (.getStorage item)))


  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader (.getStorage item) options))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->writer (.getStorage item) options))


  dtype-proto/PToIterable
  (convertible-to-iterable? [item] true)
  (->iterable [item options]
    (dtype-proto/->reader item options))


  dtype-proto/PBuffer
  (sub-buffer [item offset length]
    (dtype-proto/sub-buffer (.getStorage item) offset length))


  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-base/raw-dtype-copy! raw-data ary-target target-offset options))


  dtype-proto/PSetConstant
  (set-constant! [item offset value elem-count]
    (dtype-proto/set-constant! (.getStorage item) offset value elem-count)))
