(ns tech.libs.netcdf
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [clojure.core.matrix.protocols :as mp]
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

  mp/PElementCount
  (element-count [item] (.getSize item))

  mp/PDimensionInfo
  (dimensionality [m] (count (mp/get-shape m)))
  (get-shape [m] (vec (.getShape m)))
  (is-scalar? [m] false)
  (is-vector? [m] true)
  (dimension-count [m dimension-number]
    (let [shape (mp/get-shape m)]
      (if (<= (count shape) (long dimension-number))
        (get shape dimension-number)
        (throw (ex-info "Array does not have specific dimension"
                        {:dimension-number dimension-number
                         :shape shape})))))


  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (-> (dtype-proto/get-datatype item)
        casting/numeric-type?))
  (->buffer-backing-store [item]
    (let [byte-buf (.getDataAsByteBuffer item)]
      (case (dtype-proto/get-datatype item)
        :int8 byte-buf
        :int16 (.asShortBuffer byte-buf)
        :int32 (.asIntBuffer byte-buf)
        :int64 (.asLongBuffer byte-buf)
        :float32 (.asFloatBuffer byte-buf)
        :float64 (.asDoubleBuffer byte-buf))))


  jna/PToPtr
  (is-jna-ptr-convertible? [item]
    (dtype-proto/convertible-to-nio-buffer? item))
  (->ptr-backing-store [item]
    (when (casting/numeric-type? (dtype-proto/get-datatype item))
      (jna/as-ptr (.getDataAsByteBuffer item))))


  dtype-proto/PToReader
  (->reader-of-type [item datatype unchecked?]
    (if-let [backing-store (dtype-proto/as-nio-buffer item)]
      (dtype-proto/->reader-of-type backing-store datatype unchecked?)
      (-> (case (dtype-proto/get-datatype item)
            :boolean (reify
                       BooleanReader
                       (size [reader] (.getSize item))
                       (read [reader idx]
                         (.getBoolean item idx)))
            :object (reify ObjectReader
                      (size [reader] (.getSize item))
                      (read [reader idx]
                        (.getObject item idx))))
          (dtype-proto/->reader-of-type datatype unchecked?))))


  dtype-proto/PToWriter
  (->writer-of-type [item datatype unchecked?]
    (if-let [backing-store (dtype-proto/as-nio-buffer item)]
      (dtype-proto/->writer-of-type backing-store datatype unchecked?)
      (-> (case (dtype-proto/get-datatype item)
            :boolean (reify
                       BooleanWriter
                       (size [writer] (.getSize item))
                       (write [writer idx value]
                         (.setBoolean item idx value)))
            :object (reify ObjectWriter
                      (size [writer] (.getSize item))
                      (write [writer idx value]
                        (.setObject item idx value))))
          (dtype-proto/->reader-of-type datatype unchecked?))))


  dtype-proto/PToIterable
  (->iterable-of-type [item datatype unchecked?]
    (dtype-proto/->reader-of-type item datatype unchecked?))


  dtype-proto/PBuffer
  (sub-buffer [item offset length]
    ;;The readers created from nio buffers will sub buffer the nio buffer.
    ;;else sub-buffer is generically implemented for all readers.
    (if-let [data-buf (dtype-proto/as-nio-buffer item)]
      (dtype-proto/sub-buffer data-buf offset length)
      (-> (dtype-proto/->reader-of-type item (dtype-proto/get-datatype item) false)
          (dtype-proto/sub-buffer offset length))))


  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-base/raw-dtype-copy! raw-data ary-target target-offset options))


  dtype-proto/PSetConstant
  (set-constant! [item offset value elem-count]
    (if-let [backing-store (dtype-proto/as-nio-buffer item)]
      (dtype-proto/set-constant! backing-store offset value elem-count)
      (-> (dtype-proto/->writer-of-type item (dtype-proto/get-datatype item) false)
          (dtype-proto/set-constant! offset value elem-count)))))
