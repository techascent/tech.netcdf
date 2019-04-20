# tech.netcdf

netcdf findings into the tech ecosystem.  Based on the netcdfAll library.


[sample files](https://www.unidata.ucar.edu/software/netcdf/examples/files.html)


## Usage

```clojure

user> (require '[tech.netcdf :as netcdf])
:tech.resource.gc Reference thread starting
nil
user> (def test-file (netcdf/fname->netcdf "test/data/sresa1b_ncar_ccsm3-example.nc"))
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
#'user/test-file
user> (netcdf/variables test-file)
{"tas"
 {:name "tas",
  :dimensions
  ({:name "time", :length 1, :shared? true, :unlimited? true, :variable-length? false}
   {:name "lat",
    :length 128,
    :shared? true,
    :unlimited? false,
    :variable-length? false}
   {:name "lon",
    :length 256,
    :shared? true,
    :unlimited? false,
    :variable-length? false}),
  :attributes
  {"missing_value"
   {:name "missing_value",
    :datatype :float,
    :is-array? false,
    :is-string? false,
    :value 1.0E20}
...


user> (netcdf/overview test-file)
{"tas"
 {:name "tas",
  :shape [1 128 256],
  :data #<Delay@66492386: :not-delivered>,
  :shape-names ["time" "lat" "lon"],
  :fullname "air_temperature",
  :units "K",
  :missing-value 1.0E20},
 "ua"
 {:name "ua",
  :shape [1 17 128 256],
  :data #<Delay@388e2044: :not-delivered>,
  :shape-names ["time" "plev" "lat" "lon"],
  :fullname "eastward_wind",
  :units "m s-1",
  :missing-value 1.0E20},
 "area"
 {:name "area",
  :shape [128 256],
  :data #<Delay@2658dff1: :not-delivered>,
  :shape-names ["lat" "lon"],
  :fullname "area",
  :units "meter2",
  :missing-value nil},
 "lon"
 {:name "lon",
  :shape [256],
  :data #<Delay@4628cad3: :not-delivered>,
  :shape-names ["lon"],
  :fullname "longitude",
  :units "degrees_east",
  :missing-value nil},
 "time"
 {:name "time",
  :shape [1],
  :data #<Delay@6c0d901d: :not-delivered>,
  :shape-names ["time"],
  :fullname "time",
  :units "days since 0000-1-1",
  :missing-value nil},
 "lat"
 {:name "lat",
  :shape [128],
  :data #<Delay@206b1c4b: :not-delivered>,
  :shape-names ["lat"],
  :fullname "latitude",
  :units "degrees_north",
  :missing-value nil},
 "time_bnds"
 {:name "time_bnds",
  :shape [1 2],
  :data #<Delay@4bc0d189: :not-delivered>,
  :shape-names ["time" "bnds"],
  :fullname "time_bnds",
  :units nil,
  :missing-value nil},
 "msk_rgn"
 {:name "msk_rgn",
  :shape [128 256],
  :data #<Delay@6623b7c3: :not-delivered>,
  :shape-names ["lat" "lon"],
  :fullname "msk_rgn",
  :units "bool",
  :missing-value nil},
 "lat_bnds"
 {:name "lat_bnds",
  :shape [128 2],
  :data #<Delay@1447a9ee: :not-delivered>,
  :shape-names ["lat" "bnds"],
  :fullname "lat_bnds",
  :units nil,
  :missing-value nil},
 "lon_bnds"
 {:name "lon_bnds",
  :shape [256 2],
  :data #<Delay@24a81410: :not-delivered>,
  :shape-names ["lon" "bnds"],
  :fullname "lon_bnds",
  :units nil,
  :missing-value nil},
 "plev"
 {:name "plev",
  :shape [17],
  :data #<Delay@7d63f18a: :not-delivered>,
  :shape-names ["plev"],
  :fullname "air_pressure",
  :units "Pa",
  :missing-value nil},
 "pr"
 {:name "pr",
  :shape [1 128 256],
  :data #<Delay@2c16ecfe: :not-delivered>,
  :shape-names ["time" "lat" "lon"],
  :fullname "precipitation_flux",
  :units "kg m-2 s-1",
  :missing-value 1.0E20}}



user> (require '[tech.v2.datatype :as dtype])
nil
user> (require '[tech.v2.tensor :as tens])
nil
user> (dtype/shape test-data)
[1 128 256]
user> (dtype/get-datatype test-data)
:float32
user> (type test-data)
ucar.ma2.ArrayFloat$D3
user> (-> (tens/select test-data 0 (range 5) (range 5))
          println)
#tech.v2.tensor<float32>[5 5]
[[215.893 215.805 215.739 215.663 215.620]
 [217.168 217.030 216.910 216.792 216.632]
 [218.764 218.524 218.283 218.042 217.814]
 [219.861 219.500 219.176 218.873 218.578]
 [221.123 220.713 220.330 220.021 219.676]]
nil
user> (take 20 (dtype/->vector test-data))
(215.8935
 215.80531
 215.73935
 215.66304
 215.61963
 215.54893
 215.46086
 215.36723
 215.33812
 215.2303
 215.14465
 215.08086
 215.03511
 215.01857
 214.85776
 214.83345
 214.82996
 214.70193
 214.60558
 214.62598)
```

## License

Copyright © 2019 TechAscent, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.
