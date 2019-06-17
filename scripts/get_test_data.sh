#!/bin/bash

mkdir -p test/data
pushd test/data
wget https://www.unidata.ucar.edu/software/netcdf/examples/sresa1b_ncar_ccsm3-example.cdl
wget https://www.unidata.ucar.edu/software/netcdf/examples/sresa1b_ncar_ccsm3-example.nc
wget https://s3.us-east-2.amazonaws.com/tech.public.data/grib2-test/3600.grib2
popd
