(defproject tech.netcdf "0.1.0-SNAPSHOT"
  :description "netcdf bindings for tech ecosystem."
  :url "http://github.com/techascent/tech.netcdf"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1-beta2"]
                 [techascent/tech.datatype "4.0-alpha1"]
                 [edu.ucar/netcdfAll "4.6.13"]]
  :repositories {"bouldessgeo" "https://repo.boundlessgeo.com/artifactory/ucar-cache"}
  )
