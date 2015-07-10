# hashdb - a fast embeddable key/value database based on linear hashing

This project is under construction.

## Compiling HashDB with Visual Studio 2010

### Installing boost

If your project uses boost, you must compile HashDB with the same boost variant.
Otherwise you may either download precompiled boost binaries or compile boost from sources.

To compile HashDB with boost compiled from sources, do the following:

* Download boost 1.56 or newer and unpack it.
* Create directory for the target, e.g. "deps/boost_1_56_0"
* Run `bootstrap.bat`
* Run `bjam --prefix=...\deps\boost_1_56_0 toolset=msvc-10.0 address-model=64 --without-mpi --without-python --without-graph --without-graph_parallel --without-wave install`

The address-model should be either 32 or 64 depending on your compilation target.

### Configuring path to boost headers and libraries

* In Visual Studio 2010, open the solution build\VS2010\HashDB.sln
* Click on "Property Manager" on the bottom of the left pane (you may need to enable advanced features if you are using VS 2010 Express Edition)
* Expand the "tool" project until you find a property sheet called "boost32" or "boost64" (depending on the chosen address model), and double-click it.
* Edit C/C++ / General / Additional Include Directories, change the path to boost includes.
* Edit Linker / General / Additional Library Directories, change the to boost libraries.

### Optional step: Installing Python

A Python installation is needed only for running the unit tests. 
If you do not wish to run the unit tests, you can exclude the "tests" project from the build configuration.

Otherwise install the latest Python version from https://www.python.org/downloads/ .

### Build and test the project

Now you can build the project. 

Try to run the compiled "tool.exe".

TODO: Build also the benchmark code.

## Compiling HashDB on Linux and MacOS X

### Optional step: Installing the boost library

If your project uses boost, you must compile HashDB with the same boost variant.
Otherwise you may either use 'system' boost or compile boost from sources.

To compile HashDB with boost compiled from sources, do the following:

* Download boost 1.56 or newer and unpack it.
* Create directory for the target, e.g. "deps/boost_1_56_0"
* Run `./bootstrap.sh --prefix=..../deps/boost_1_56_0`
* Run `./b2 --without-mpi --without-python --without-graph --without-graph_parallel --without-wave install`

### Step 1: Configuring HashDB

* Run `./configure.sh` in the root of the HashDB project

TODO
