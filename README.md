hashdb - a fast embeddable key/value database
=============================================

* embeddable key/value database library written in C++ with a C++ API
* provides fast storage and retrieval - the database is based on the [linear hashing algorithm](https://en.wikipedia.org/wiki/Linear_hashing)
* cross platform - the database has been thoroughly tested on Windows, Linux and MacOS X
* open source with a very permissive MIT-like [open source license](LICENSE.txt)

Status: Windows build is now complete, but new build scripts for POSIX platforms are not yet finished.

Sample code:

```C++
#include <iostream>
#include <kerio/hashdb/HashDB.h>
#include <kerio/hashdb/Exception.h>

int main(int argc, char* argv)
{
    using namespace kerio::hashdb;

    try {
        // Open the database.
        Database db = DatabaseFactory();
        db->open("sample", Options::readWriteSingleThreaded());

        // Store three values.
        db->store("key1", 0, "value1");

        db->store("key2", 0, "value2");
        db->store("key2", 1, "value3"); // "value2" and "value3" gets stored to the same bucket.

        // List the values.
        for (Iterator it = db->newIterator(); it->isValid(); it->next()) {
            std::cout << "key=" << it->key() << ", "
                      << "part=" << it->partNum() << ", "
                      << "value=" << it->value() << std::endl;
        }
    }
    catch (const kerio::hashdb::Exception& e) {
        std::cout << "Exception: " << e.what() << std::endl;
    }
}
```


Compiling HashDB with Visual Studio 2010
----------------------------------------

### 1. Installing boost

If your project uses boost, you must compile HashDB with the same boost variant.
Otherwise you may either download precompiled boost binaries or compile boost from sources.

To compile HashDB with boost compiled from sources, do the following:

* Download boost 1.56 or newer and unpack it.
* Create directory for the target, e.g. "deps/boost_1_56_0"
* Run `bootstrap.bat`
* Run `bjam --prefix=...\deps\boost_1_56_0 toolset=msvc-10.0 address-model=64 --without-mpi --without-python --without-graph --without-graph_parallel --without-wave install`

The address-model should be either 32 or 64 depending on your compilation target.

### 2. Configuring path to boost headers and libraries

* In Visual Studio 2010, open the solution build\VS2010\HashDB.sln
* Click on "Property Manager" on the bottom of the left pane (you may need to enable advanced features if you are using VS 2010 Express Edition)
* Expand the "tool" project until you find a property sheet called "boost32" or "boost64" (depending on the chosen address model), and double-click it.
* Edit C/C++ / General / Additional Include Directories, change the path to boost includes.
* Edit Linker / General / Additional Library Directories, change the to boost libraries.

### 3. Optional step: Installing Python

A Python installation is needed only for running the unit tests. 
If you do not wish to run the unit tests, you can exclude the "tests" project from the build configuration.

Otherwise install the latest Python version from https://www.python.org/downloads/ .

### 4. Build and test the database library

Now you can build the project. Check that all the unit tests succeeded in the build output window.

### 5. Optional step: Build the sample code

* Add a new console application project to the solution
* In Property Manager, add property sheets PropSheet\hashdb_headers.props and PropSheet\boost32.props (or boost64) to the new project
* Add references to the "db" and "utils" projects to the new project
* Copy the above sample code
* Compile and run.


Compiling HashDB on Linux and MacOS X
-------------------------------------

### 1. Optional step: Installing the boost library

If your project uses boost, you must compile HashDB with the same boost variant.
Otherwise you may either use 'system' boost or compile boost from sources.

To compile HashDB with boost compiled from sources, do the following:

* Download boost 1.56 or newer and unpack it.
* Create directory for the target, e.g. "deps/boost_1_56_0"
* Run `./bootstrap.sh --prefix=..../deps/boost_1_56_0`
* Run `./b2 --without-mpi --without-python --without-graph --without-graph_parallel --without-wave install`

### 2. Configuring HashDB

* Run `./configure.sh` in the root of the HashDB project

TODO: Build scripts for POSIX.
