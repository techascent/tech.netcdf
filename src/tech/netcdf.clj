(ns tech.netcdf
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [tech.resource :as resource]
            [tech.libs.netcdf :as lib-netcdf]
            [tech.v2.tensor :as dtt]
            [tech.v2.tensor.typecast :as dtt-typecast]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.v2.datatype.readers.const :as const-rdr]
            [clojure.pprint :as pp])
  (:import [ucar.nc2.dataset NetcdfDataset CoordinateAxis1D CoordinateSystem]
           [ucar.nc2 Dimension NetcdfFile Attribute Variable]
           [ucar.unidata.geoloc ProjectionImpl LatLonRect LatLonPointImpl
            Projection ProjectionPointImpl]
           [ucar.unidata.geoloc.projection LambertConformal]
           [ucar.nc2.dt.grid GridDataset GridDataset$Gridset]
           [ucar.nc2.dt GridDatatype GridCoordSystem]
           [ucar.ma2 DataType]
           [tech.v2.tensor FloatTensorReader]
           [tech.v2.datatype IntReader FloatReader]
           [tech.netcdf LerpOp FastLambertConformal]
           [java.util List RandomAccess]))


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
              (assoc (select-keys vardata [:name :shape :data :datatype])
                     :shape-names (mapv :name dimensions)
                     :fullname (get-in attributes ["standard_name" :value]
                                       (:name vardata))
                     :units (get-in attributes ["units" :value])
                     :missing-value (get-in attributes ["missing_value" :value]))))
       named-item->map))


(defmulti format-data-sequence
  (fn [datatype _data-seq]
    datatype))


(defmethod format-data-sequence :default
  [_datatype data-seq]
  (str (vec data-seq)))


(defmethod format-data-sequence :float
  [_datatype data-seq]
  (str (mapv #(format "%3.2e" %) data-seq)))


(defmethod format-data-sequence :double
  [_datatype data-seq]
  (str (mapv #(format "%3.2e" %) data-seq)))


(defn format-scalar
  [dtype s-val]
  (when s-val
    (format-data-sequence dtype [s-val])))


(defn- elipsoid
  [str-value ^long num-digits]
  (when (string? str-value)
    (if (>= (count str-value) num-digits)
      (str (.substring ^String str-value 0 7) "...")
      str-value)))


(defn valid-data-indexes
  [data fill-val missing-val]
  (let [fill-val (double (or fill-val missing-val))
        missing-val (double (or missing-val fill-val))
        ddata (dtype/->reader data :float64)]
    (-> (dfn/argfilter
         (boolean-op/make-boolean-unary-op
          :float64
          (not (or (= 0 (Double/compare fill-val x))
                   (= 0 (Double/compare missing-val x)))))
         ddata)
        long-array)))


(defn valid-data
  [data fill-val missing-val]
  (if (and data (or fill-val missing-val))
    (let [indexes (valid-data-indexes data fill-val missing-val)]
      (indexed-rdr/make-indexed-reader indexes data {}))
    (dtype/->reader data)))


(defn print-variable-table
  [netcdf & {:keys [return-varmap?]}]
  (resource/stack-resource-context
   (let [ncfile (if (string? netcdf)
                  (fname->netcdf netcdf)
                  netcdf)
         vars (->> (vals (variables ncfile))
                   (sort-by :name))
         varmap
         (->> vars
              (map (fn [{:keys [datatype shape name attributes data]}]
                     (let [vdata (valid-data @data
                                             (get-in attributes
                                                     ["_FillValue" :value])
                                             (get-in attributes
                                                     ["missing_value" :value]))]
                       {:name name
                        :datatype datatype
                        :shape shape
                        :vdata vdata
                        :units (-> (get-in attributes ["units" :value])
                                   (elipsoid 12))
                        :valid-ecount (dtype/ecount vdata)
                        :example-values
                        (try (->> vdata
                                  (take 3)
                                  (format-data-sequence datatype))
                             (catch Throwable e (format "Error: %s" e)))}))))]
     (pp/print-table [:name :units :datatype :shape
                      :valid-ecount :example-values] varmap)
     (if return-varmap?
       varmap
       :ok))))


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
  [fname & {:keys [time-axis z-axis fill-value datatype container-type]
            :or {time-axis 0 z-axis 0 datatype :float32
                 container-type :java-array}}]
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
                                      :container-type container-type))]
                        {:name (.getName %)
                         :attributes atts
                         :data data
                         :data-reader (dtt-typecast/->float32-reader data)
                         :coordinate-system (.getGeoCoordSystem gridset)}))))))
           vec))))


(def test-file "test/data/3600.grib2")
(def test-lat-lng [[21.145 237.307]])


(defn- to-rand-access
  ^List [item]
  (if (instance? RandomAccess item)
    item
    (vec item)))


(defn serial-lat-lng->proj
  "Serially project a sequence of lat-lng tuples to a projection.  Returns
  a tuple if [^floats x-coords ^floats y-coords]"
  [^ProjectionImpl projection lat-lng-seq]
  (let [lat-lng-seq (to-rand-access lat-lng-seq)
        n-elems (.size lat-lng-seq)
        lat-ary (-> (reify FloatReader
                      (lsize [_] n-elems)
                      (read [_ idx] (first (.get lat-lng-seq idx))))
                    (dtype/copy! (float-array n-elems)))
        lng-ary (-> (reify FloatReader
                      (lsize [_] n-elems)
                      (read [_ idx] (second (.get lat-lng-seq idx))))
                    (dtype/copy! (float-array n-elems)))
        x-coords (float-array n-elems)
        y-coords (float-array n-elems)]
    (.latLonToProj projection
                   ^"[[F" (into-array [lat-ary lng-ary])
                   ^"[[F" (into-array [x-coords y-coords])
                   0 1)
    [x-coords y-coords]))


(defn parallel-lat-lng->proj
  [projection lat-lng-seq]
  (let [lat-lng-seq (to-rand-access lat-lng-seq)
        n-elems (.size lat-lng-seq)]
    ;;It has to be worth combining these things back again.
    (if (< n-elems 1000)
      (serial-lat-lng->proj projection lat-lng-seq)
      (let [n-cpus (+ 2 (.availableProcessors (Runtime/getRuntime)))
            n-elems-per-list (+ 1 (quot n-elems n-cpus))
            results
            (->> (range n-cpus)
                 (pmap
                  (fn [cpu-idx]
                    (let [cpu-idx (long cpu-idx)
                          start-offset (* n-elems-per-list cpu-idx)
                          end-offset (min (+ start-offset n-elems-per-list)
                                          n-elems)]
                      (serial-lat-lng->proj projection
                                            (.subList lat-lng-seq
                                                      start-offset
                                                      end-offset))))))
            x-coords (float-array n-elems)
            y-coords (float-array n-elems)]
        (dtype/copy-raw->item! (map first results) x-coords)
        (dtype/copy-raw->item! (map second results) y-coords)
        [x-coords y-coords]))))


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
   :cell-lat-lngs - reader of [lat lng] tuples of the center points of the grid cells
   :values - (float-array n-elems)
  }"
  [{:keys [coordinate-system name attributes data-reader]}
   lat-lon-seq]
  (let [lat-lng-seq (to-rand-access lat-lon-seq)
        n-elems (count lat-lng-seq)
        ^GridCoordSystem coordinate-system coordinate-system
        ^CoordinateAxis1D x-axis (.getXHorizAxis coordinate-system)
        ^CoordinateAxis1D y-axis (.getYHorizAxis coordinate-system)
        [x-coords y-coords] (-> (.getProjection coordinate-system)
                                (parallel-lat-lng->proj lat-lng-seq))
        ^floats x-coords x-coords
        ^floats y-coords y-coords
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


(defn- get-field
  [item fname]
  (when item
    (when-let [field (.getDeclaredField ^Class (type item) fname)]
      (.setAccessible field true)
      (.get field item))))

(defn- coord-1d
  ^CoordinateAxis1D [item]
  (if (instance? CoordinateAxis1D item)
    item
    (throw (Exception. (format "Axis is not a coord1d axis: %s" (type item))))))


(defn make-fast-lat-lon-projector
  ^Projection [^Projection proj]
  (if (instance? LambertConformal proj)
    (let [fast-proj (FastLambertConformal. proj)]
      (reify
        Projection
        (latLonToProj [lhs-proj latlon-pt proj-pt-impl]
          (.latLonToProj fast-proj latlon-pt proj-pt-impl))))
    proj))


(defn setup-nearest-projection
  [grid]
  (let [^GridCoordSystem coords (:coordinate-system grid)
        proj (.getProjection coords)
        x-axis (.getXHorizAxis coords)
        y-axis (.getYHorizAxis coords)
        x-coords (when x-axis (get-field x-axis "coords"))
        y-coords (when y-axis (get-field y-axis "coords"))]
    (if (and (instance? CoordinateAxis1D x-axis)
             (instance? CoordinateAxis1D y-axis)
             (.isRegular ^CoordinateAxis1D x-axis)
             (.isRegular ^CoordinateAxis1D y-axis)
             x-coords
             y-coords)
      (let [x-min (double (first x-coords))
            x-max (double (last x-coords))
            n-x (count x-coords)
            nn-x (dec n-x)
            y-min (double (first y-coords))
            y-max (double (last y-coords))
            n-y (count y-coords)
            nn-y (dec n-y)
            x-range (- x-max x-min)
            y-range (- y-max y-min)
            x-mult (/ nn-x x-range)
            y-mult (/ nn-y y-range)
            data (dtt-typecast/->float32-reader (:data grid))
            proj (make-fast-lat-lon-projector proj)]
        (fn [lat-lng-seq]
          (let [lat-lng-seq (to-rand-access lat-lng-seq)
                n-elems (.size lat-lng-seq)]
            (reify FloatReader
              (lsize [rdr] n-elems)
              (read [rdr idx]
                (let [ll-data (typecast/datatype->reader :float64
                                                         (.get lat-lng-seq idx))
                      lat (.read ll-data 0)
                      lng (.read ll-data 1)
                      ^ProjectionPointImpl proj-point
                      (.latLonToProj proj (LatLonPointImpl. lat lng)
                                     (ProjectionPointImpl.))
                      grid-idx-x (-> (- (.getX proj-point) x-min)
                                     (* x-mult)
                                     (Math/round)
                                     long
                                     (min nn-x)
                                     (max 0))
                      grid-idx-y (-> (- (.getY proj-point) y-min)
                                     (* y-mult)
                                     (Math/round)
                                     long
                                     (min nn-y)
                                     (max 0))]
                  (.read2d data grid-idx-y grid-idx-x)))))))
      (fn [lat-lng-seq]
        (let [lat-lng-seq (to-rand-access lat-lng-seq)
              n-elems (.size lat-lng-seq)
              data (dtt-typecast/->float32-reader (:data grid))
              x-axis (coord-1d x-axis)
              y-axis (coord-1d y-axis)]
          (reify FloatReader
            (lsize [rdr] n-elems)
            (read [rdr idx]
              (let [ll-data (typecast/datatype->reader :float64 (.get lat-lng-seq idx))
                    lat (.read ll-data 0)
                    lng (.read ll-data 1)
                    proj-point (.latLonToProj proj (LatLonPointImpl. lat lng))
                    grid-idx-x (.findCoordElementBounded x-axis (.getX proj-point))
                    grid-idx-y (.findCoordElementBounded y-axis (.getY proj-point))]
                (.read2d data grid-idx-y grid-idx-x)))))))))


(def default-lerp-op
  (reify LerpOp
    (lerp [this lhs-weight lhs rhs-weight rhs]
      (+ (* lhs-weight lhs)
         (* rhs-weight rhs)))))


(defn- to-lerp-op
  ^LerpOp [item]
  (if (instance? LerpOp item)
    item
    (throw (Exception. (format "Op is not a lerp op: %s" (type item))))))


(defn setup-linear-interpolator
  [lhs-grid rhs-grid & [lerp-op]]
  (let [lerp-op (to-lerp-op (or lerp-op default-lerp-op))
        ^GridCoordSystem lhs-coords (:coordinate-system lhs-grid)
        ^GridCoordSystem rhs-coords (:coordinate-system rhs-grid)
        lhs-proj (.getProjection lhs-coords)
        rhs-proj (.getProjection rhs-coords)
        lhs-axis [(.getXHorizAxis lhs-coords) (.getYHorizAxis lhs-coords)]
        rhs-axis [(.getXHorizAxis rhs-coords) (.getYHorizAxis rhs-coords)]
        lhs-x-coords (get-field (first lhs-axis) "coords")
        lhs-y-coords (get-field (second lhs-axis) "coords")
        rhs-x-coords (get-field (first rhs-axis) "coords")
        rhs-y-coords (get-field (second rhs-axis) "coords")
        lhs-axis-data [(first lhs-x-coords) (first lhs-y-coords)
                       (last lhs-x-coords) (last lhs-y-coords)]
        rhs-axis-data [(first rhs-x-coords) (first rhs-y-coords)
                       (last rhs-x-coords) (last rhs-y-coords)]
        lhs-data (dtt-typecast/->float32-reader (:data lhs-grid))
        rhs-data (dtt-typecast/->float32-reader (:data rhs-grid))]
    ;;fastpath for same projection and both regular projections.
    (if (and (= lhs-proj rhs-proj)
               (every? #(and (not (nil? %))
                             (.isRegular ^CoordinateAxis1D %))
                       (concat lhs-axis rhs-axis))
               (dfn/equals lhs-axis-data rhs-axis-data))
      (let [[x-min y-min x-max y-max] lhs-axis-data
            x-min (double x-min)
            y-min (double y-min)
            x-max (double x-max)
            y-max (double y-max)
            x-range (- x-max x-min)
            y-range (- y-max y-min)
            n-x (count lhs-x-coords)
            n-y (count lhs-y-coords)
            nn-x (dec n-x)
            nn-y (dec n-y)
            x-mult (/ nn-x x-range)
            y-mult (/ nn-y y-range)
            lhs-proj (make-fast-lat-lon-projector lhs-proj)]
        (fn [lat-lng-seq lhs-weights rhs-weights]
          (let [lat-lng-seq (to-rand-access lat-lng-seq)
                n-elems (.size lat-lng-seq)
                lhs-weights (if (number? lhs-weights)
                              (const-rdr/make-const-reader lhs-weights :float32)
                              lhs-weights)
                rhs-weights (if (number? rhs-weights)
                              (const-rdr/make-const-reader rhs-weights :float32)
                              lhs-weights)
                lhs-weights (typecast/datatype->reader :float32 lhs-weights)
                rhs-weights (typecast/datatype->reader :float32 rhs-weights)]
            (reify FloatReader
              (lsize [rdr] n-elems)
              (read [rdr idx]
                (let [data (typecast/datatype->reader :float64 (.get lat-lng-seq idx))
                      lat (.read data 0)
                      lng (.read data 1)
                      ^ProjectionPointImpl proj-point
                      (.latLonToProj lhs-proj (LatLonPointImpl. lat lng)
                                     (ProjectionPointImpl.))
                      grid-idx-x (-> (- (.getX proj-point) x-min)
                                     (* x-mult)
                                     (Math/round)
                                     long
                                     (min nn-x)
                                     (max 0))
                      grid-idx-y (-> (- (.getY proj-point) y-min)
                                     (* y-mult)
                                     (Math/round)
                                     long
                                     (min nn-y)
                                     (max 0))]
                  (.lerp lerp-op
                         (.read lhs-weights idx)
                         (.read2d lhs-data grid-idx-y grid-idx-x)
                         (.read rhs-weights idx)
                         (.read2d rhs-data grid-idx-y grid-idx-x))))))))
      (fn [lat-lng-seq lhs-weights rhs-weights]
        (let [lat-lng-seq (to-rand-access lat-lng-seq)
              n-elems (.size lat-lng-seq)
              lhs-weights (if (number? lhs-weights)
                            (const-rdr/make-const-reader lhs-weights :float32)
                            lhs-weights)
              rhs-weights (if (number? rhs-weights)
                            (const-rdr/make-const-reader rhs-weights :float32)
                            lhs-weights)
              lhs-weights (typecast/datatype->reader :float32 lhs-weights)
              rhs-weights (typecast/datatype->reader :float32 rhs-weights)
              lhs-x-axis (coord-1d (.getXHorizAxis lhs-coords))
              lhs-y-axis (coord-1d (.getYHorizAxis lhs-coords))
              rhs-x-axis (coord-1d (.getXHorizAxis rhs-coords))
              rhs-y-axis (coord-1d (.getYHorizAxis rhs-coords))]
          (reify FloatReader
            (lsize [rdr] n-elems)
            (read [rdr idx]
              (let [data (typecast/datatype->reader :float64 (.get lat-lng-seq idx))
                    lat (.read data 0)
                    lng (.read data 1)
                    lhs-proj-point (.latLonToProj lhs-proj (LatLonPointImpl. lat lng))
                    rhs-proj-point (.latLonToProj rhs-proj (LatLonPointImpl. lat lng))
                    lhs-x-idx (.findCoordElementBounded lhs-x-axis
                                                        (.getX lhs-proj-point))
                    lhs-y-idx (.findCoordElementBounded lhs-y-axis
                                                        (.getY lhs-proj-point))
                    rhs-x-idx (.findCoordElementBounded rhs-x-axis
                                                        (.getX rhs-proj-point))
                    rhs-y-idx (.findCoordElementBounded rhs-y-axis
                                                        (.getY rhs-proj-point))]
                (.lerp lerp-op
                       (.read lhs-weights idx)
                       (.read2d lhs-data lhs-y-idx lhs-x-idx)
                       (.read rhs-weights idx)
                       (.read2d rhs-data rhs-y-idx rhs-x-idx))))))))))
