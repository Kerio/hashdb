// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#pragma once

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
#define NOMINMAX						// No min and max macros
#endif

#include <cstdlib>
#include <string>
#include <boost/utility/string_ref.hpp>

#if defined(_WIN32)
#include <Windows.h>
#endif
