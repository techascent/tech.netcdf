(ns tech.netcdf.maptiles
  "https://docs.microsoft.com/en-us/azure/azure-maps/zoom-levels-and-tile-grid?tabs=csharp
  Transform from maptile space to lat-lng space.")


(defn earth-radius ^double [] 6378137.0)

(defn min-latitude ^double [] -85.05112878)
(defn max-latitude ^double []  85.05112878)
(defn min-longtiude ^double [] -180.0)
(defn max-longitude ^double [] 180.0)


(defn clip
  ^double [^double n ^double min-val ^double max-val]
  (min (max n min-val) max-val))

(defn lng-clip
  "Return longitude in the range of [-180 180]"
  ^double [^double lng]
  (loop [lng lng]
    (if (or (> lng 180.0)
            (< lng -180.0))
      (let [addition (if (> lng 0)
                       (- 360.0)
                       360.0)]
        (recur (+ lng addition)))
      lng)))


(defn map-size
  ^double [zoom tile-size]
  (-> (* (long tile-size) (Math/pow 2.0 (double zoom)))
      (Math/ceil)))


;;return Math.Cos(latitude * Math.PI / 180)
;;* 2 * Math.PI * EarthRadius
;;/ MapSize(zoom, tileSize);



(defn ground-resolution
  "Calculates the Ground resolution at a specific degree of latitude in
  meters per pixel."
  ^double [lat zoom tile-size]
  (let [latitude (clip lat (min-latitude) (max-latitude))]
    (-> (Math/cos (* latitude (/ Math/PI 180)))
        (* 2.0 Math/PI (earth-radius))
        (/ (map-size zoom tile-size)))))


;; public static double[] GlobalPixelToPosition(double[] pixel, double zoom, int tileSize)
;;         {
;;             var mapSize = MapSize(zoom, tileSize);

;;             var x = (Clip(pixel[0], 0, mapSize - 1) / mapSize) - 0.5;
;;             var y = 0.5 - (Clip(pixel[1], 0, mapSize - 1) / mapSize);

;;             return new double[] {
;;                 360 * x,    //Longitude
;;                 90 - 360 * Math.Atan(Math.Exp(-y * 2 * Math.PI)) / Math.PI  //Latitude
;;             };
;;         }

(defn pixel->lng-lat
  "Given a pixel position on the map, get the lat/long that applies to that pixel."
  [pixel zoom tile-size]
  (let [[pix-x pix-y] pixel
        map-size (map-size zoom tile-size)
        x (-> (clip pix-x 0 (- map-size 1))
              (/ map-size)
              (- 0.5))
        y (- 0.5 (-> (clip pix-y 0 (- map-size 1))
                     (/ map-size)))]
    [(* 360.0 x)
     (- 90 (-> (* (- y) 2 Math/PI)
               (Math/exp)
               (Math/atan)
               (/ Math/PI)
               (* 360.0)))]))


 ;; public static double[] PositionToGlobalPixel(double[] position, int zoom, int tileSize)
 ;;        {
 ;;            var latitude = Clip(position[1], MinLatitude, MaxLatitude);
 ;;            var longitude = Clip(position[0], MinLongitude, MaxLongitude);

 ;;            var x = (longitude + 180) / 360;
 ;;            var sinLatitude = Math.Sin(latitude * Math.PI / 180);
 ;;            var y = 0.5 - Math.Log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

 ;;            var mapSize = MapSize(zoom, tileSize);

 ;;            return new double[] {
 ;;                 Clip(x * mapSize + 0.5, 0, mapSize - 1),
 ;;                 Clip(y * mapSize + 0.5, 0, mapSize - 1)
 ;;            };
 ;;        }

(defn lng-lat->pixel
  "Convert lat/long->pixel location in map space at specific zoom level."
  [position zoom tile-size]
  (let [[x-pos y-pos] position
        latitude (clip y-pos (min-latitude) (max-latitude))
        longitude (lng-clip x-pos)
        ;;X is just linearly interpolated
        x (-> (+ longitude 180)
              (/ 360))
        sin-lat (Math/sin (Math/toRadians latitude))
        y (- 0.5
             (-> (/ (+ 1 sin-lat) (- 1 sin-lat))
                 (Math/log)
                 (/ (* 4 Math/PI))))
        map-size (map-size zoom tile-size)]
    [(clip (+ 0.5 (* x map-size)) 0 (- map-size 1))
     (clip (+ 0.5 (* y map-size)) 0 (- map-size 1))]))


(defn pixel->tile-index
  [pixel tile-size]
  (let [[pix-x pix-y] pixel
        tile-size (double tile-size)]
    [(long (/ (double pix-x) tile-size))
     (long (/ (double pix-y) tile-size))]))


(defn tile-index->lat-lng-bbox
  [tile-index zoom tile-size]
  (let [[tile-x tile-y] tile-index
        tile-size (long tile-size)
        min-pix-x (* (long tile-x) tile-size)
        min-pix-y (* (long tile-y) tile-size)
        max-pix-x (+ min-pix-x tile-size)
        max-pix-y (+ min-pix-y tile-size)
        [min-lng min-lat] (pixel->lng-lat [min-pix-x min-pix-y] zoom tile-size)
        [max-lng max-lat] (pixel->lng-lat [max-pix-x max-pix-y] zoom tile-size)]
    [(min min-lng max-lng) (min min-lat max-lat)
     (max min-lng max-lng) (max min-lat max-lat)]))
