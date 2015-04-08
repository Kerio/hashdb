/* Copyright (c) 2015 Kerio Technologies s.r.o.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE
 * LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES, OR
 * ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder shall not
 * be used in advertising or otherwise to promote the sale, use or other
 * dealings in this Software without prior written authorization of the
 * copyright holder.
 */

// ExceptionImpl.h - implementation of hashdb exceptions and support functions.
#pragma once
#include <cstdarg>
#include <kerio/hashdb/Exception.h>
#include "StringUtils.h"

namespace kerio {
namespace hashdb {

	template <class T>
	class ExceptionCreator { // intentionally copyable
	public:
		ExceptionCreator(const boost::string_ref& file, int line, const boost::string_ref& function)
			: file_(file)
			, line_(line)
			, function_(function)
		{
			
		}

		void operator()(const std::string& message)
		{
			throw T(file_, line_, function_, message);
		}

	private:
		const boost::string_ref file_;
		const int line_;
		const boost::string_ref function_;
	};

}; // namespace hashdb
}; // namespace kerio

//-----------------------------------------------------------------------------
// InvalidArgumentException

#define RAISE_INVALID_ARGUMENT(fmt, ...)																						\
	do {																														\
		const std::string message = formatMessage(fmt, ## __VA_ARGS__);															\
		kerio::hashdb::ExceptionCreator<kerio::hashdb::InvalidArgumentException>(__FILE__, __LINE__, __FUNCTION__)(message);	\
	} while (false)


#define RAISE_INVALID_ARGUMENT_IF(expr, fmt, ...)																				\
	do {																														\
		if (expr) {																												\
			const std::string message = formatMessage(fmt, ## __VA_ARGS__);														\
			kerio::hashdb::ExceptionCreator<kerio::hashdb::InvalidArgumentException>(__FILE__, __LINE__, __FUNCTION__)(message); \
		}																														\
	} while (false)

//-----------------------------------------------------------------------------
// DatabaseCorruptedException

#define RAISE_DATABASE_CORRUPTED(fmt, ...)																						\
	do {																														\
		const std::string message = formatMessage(fmt, ## __VA_ARGS__);															\
		kerio::hashdb::ExceptionCreator<kerio::hashdb::DatabaseCorruptedException>(__FILE__, __LINE__, __FUNCTION__)(message);	\
	} while (false)

#define RAISE_DATABASE_CORRUPTED_IF(expr, fmt, ...)																				\
	do {																														\
		if (expr) {																												\
			const std::string message = formatMessage(fmt, ## __VA_ARGS__);														\
			kerio::hashdb::ExceptionCreator<kerio::hashdb::DatabaseCorruptedException>(__FILE__, __LINE__, __FUNCTION__)(message); \
		}																														\
	} while (false)

//-----------------------------------------------------------------------------
// IncompatibleDatabaseVersion

#define RAISE_INCOMPATIBLE_DATABASE(fmt, ...)																					\
	do {																														\
		const std::string message = formatMessage(fmt, ## __VA_ARGS__);															\
		kerio::hashdb::ExceptionCreator<kerio::hashdb::IncompatibleDatabaseVersion>(__FILE__, __LINE__, __FUNCTION__)(message);	\
	} while (false)

//-----------------------------------------------------------------------------
// ValueTooLarge

#define RAISE_VALUE_TOO_LARGE_IF(expr, fmt, ...)																				\
	do {																														\
		if (expr) {																												\
			const std::string message = formatMessage(fmt, ## __VA_ARGS__);														\
			kerio::hashdb::ExceptionCreator<kerio::hashdb::ValueTooLarge>(__FILE__, __LINE__, __FUNCTION__)(message);			\
		}																														\
	} while (false)

//-----------------------------------------------------------------------------
// IoException

#define RAISE_IO_ERROR(fmt, ...)																								\
	do {																														\
		const std::string message = formatMessage(fmt, ## __VA_ARGS__);															\
		kerio::hashdb::ExceptionCreator<kerio::hashdb::IoException>(__FILE__, __LINE__, __FUNCTION__)(message);					\
	} while(false)

#define RAISE_IO_ERROR_IF(expr, fmt, ...)																						\
	do {																														\
		if (expr) {																												\
			const std::string message = formatMessage(fmt, ## __VA_ARGS__);														\
			kerio::hashdb::ExceptionCreator<kerio::hashdb::IoException>(__FILE__, __LINE__, __FUNCTION__)(message);				\
		}																														\
	} while (false)

//-----------------------------------------------------------------------------
// NotYetImplementedException

#define RAISE_NOT_YET_IMPLEMENTED(fmt, ...)																						\
	do {																														\
		const std::string message = formatMessage(fmt, ## __VA_ARGS__);															\
		kerio::hashdb::ExceptionCreator<kerio::hashdb::NotYetImplementedException>(__FILE__, __LINE__, __FUNCTION__)(message);	\
	} while (false)

//-----------------------------------------------------------------------------
// InternalErrorException

#define RAISE_INTERNAL_ERROR(fmt, ...)																							\
	do {																														\
		const std::string message = formatMessage(fmt, ## __VA_ARGS__);															\
		kerio::hashdb::ExceptionCreator<kerio::hashdb::InternalErrorException>(__FILE__, __LINE__, __FUNCTION__)(message);		\
	} while (false)

#define RAISE_INTERNAL_ERROR_IF(expr, fmt, ...)																					\
	do {																														\
		if (expr) {																												\
			const std::string message = formatMessage(fmt, ## __VA_ARGS__);														\
			kerio::hashdb::ExceptionCreator<kerio::hashdb::InternalErrorException>(__FILE__, __LINE__, __FUNCTION__)(message);	\
		}																														\
	} while (false)

#define RAISE_INTERNAL_ERROR_IF_ARG(expr)																						\
	do {																														\
		if (expr) {																												\
			kerio::hashdb::ExceptionCreator<kerio::hashdb::InternalErrorException>(__FILE__, __LINE__, __FUNCTION__)(#expr);	\
		}																														\
	} while (false)
