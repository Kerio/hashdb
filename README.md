# hashdb

This project is under construction.

## Installing HashDB on Linux

### Optional step: Installing boost

To compile HashDB with your own boost version, do the following:

* Download boost 1.53 or newer and unpack it.
* Create directory for the target, e.g. "deps/boost_1_53_0"
* Run `./bootstrap.sh --prefix=..../deps/boost_1_53_0`
* Run `./b2 --without-mpi --without-python --without-graph --without-graph_parallel --without-wave install`

