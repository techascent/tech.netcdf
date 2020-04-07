(defproject techascent/tech.netcdf "0.89-SNAPSHOT"
  :description "netcdf bindings for tech ecosystem."
  :url "http://github.com/techascent/tech.netcdf"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :plugins [[lein-tools-deps "0.4.5"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-nop "1.7.25"]]}}
  :java-source-paths ["java"]
  :repositories {"boundlessgeo" "https://repo.boundlessgeo.com/artifactory/ucar-cache"})
