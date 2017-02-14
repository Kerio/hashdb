@echo off
REM  Install wget, edit the variables and run this script to fetch and compile boost libraries for Windows.
SETLOCAL EnableDelayedExpansion

REM  ******************************************************************
REM  Boost version to use, e.g. "1_62_0".
set BOOST_VERSION=1_62_0

REM  Address model: "32" or "64".
set ADDRESS_MODEL=64

REM  MSVC toolset: "10.0" = Visual Studio 2010, "14.0" = Visual Studio 2015
set MSVC_TOOLSET=10.0

REM  Archive name.
set BOOST_ARCHIVE_NAME=boost_%BOOST_VERSION%.zip

REM  Boost source download URL.
set BOOST_DOWNLOAD_URL=https://sourceforge.net/projects/boost/files/boost/1.62.0/%BOOST_ARCHIVE_NAME%

REM  Boost local sources directory (..\deps\boost_source)
set DEPS_DIR=deps
set BOOST_SRC=boost_source

REM  Boost target dir.
set BOOST_DST=boost_%BOOST_VERSION%_vs%MSVC_TOOLSET%_%ADDRESS_MODEL%

REM  ******************************************************************

wget --help > nul
if ERRORLEVEL 1 (
    echo "Please install the wget binary first"
    goto EndLabel
)

unzip --help > nul
if ERRORLEVEL 1 (
    echo "Please install the unzip binary first"
    goto EndLabel
)

REM  ******************************************************************

echo * Creating directories...
mkdir ..\%DEPS_DIR%
mkdir ..\%DEPS_DIR%\%BOOST_SRC%

echo * Switching to ..\%DEPS_DIR%\%BOOST_SRC%...
cd ..\%DEPS_DIR%\%BOOST_SRC%
if ERRORLEVEL 1 (
    echo ERROR: Unable to switch to directory ..\%DEPS_DIR%\%BOOST_SRC%
    goto EndLabel
)

if not exist "%BOOST_ARCHIVE_NAME%" (
    echo * Downloading boost source...
    wget %BOOST_DOWNLOAD_URL%
    if ERRORLEVEL 1 (
        echo ERROR: Unable to fetch boost sources from %BOOST_DOWNLOAD_URL%
        goto EndLabel
    )
) else (
    echo * Skipping download, archive %BOOST_ARCHIVE_NAME% already exists.
)

if not exist boost_%BOOST_VERSION% (
    echo * Unpacking boost source...
    unzip "%BOOST_ARCHIVE_NAME%"
    if ERRORLEVEL 1 (
        echo ERROR: Unable to UNZIP boost sources
        goto EndLabel
    )
) else (
    echo * Directory ..\%DEPS_DIR%\%BOOST_SRC%\boost_%BOOST_VERSION% exists, skipping UNZIP.
)

cd boost_%BOOST_VERSION%
if ERRORLEVEL 1 (
    echo ERROR: Unable to switch to %BOOST_VERSION%
    goto EndLabel
)

echo * Running boost bootstrap.bat...
call bootstrap.bat
if ERRORLEVEL 1 (
    echo "ERROR: boost bootstrap.bat failed"
    goto EndLabel
)

echo * Creating directory ..\..\%BOOST_DST%
mkdir ..\..\%BOOST_DST%

echo * Compiling boost...
bjam --prefix=..\..\%BOOST_DST% toolset=msvc-%MSVC_TOOLSET% address-model=%ADDRESS_MODEL% --without-mpi --without-python --without-graph --without-graph_parallel --without-wave install
if ERRORLEVEL 1 (
    echo ERROR: compiling boost to %BOOST_DST% failed
    goto EndLabel
)

echo * boost library successfully compiled to %BOOST_DST%

:EndLabel
ENDLOCAL
