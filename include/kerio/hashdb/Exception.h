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

// Exception.h -- exceptions thrown by hashdb.
#pragma once
#include <exception>
#include <string>
#include <boost/utility/string_ref.hpp>

namespace kerio {
namespace hashdb {

	// Base exception for the exceptions thrown by hashdb.
	class Exception : public std::exception
	{
	public:
		virtual const char* what() const throw()
		{
			return what_.c_str();
		}

		virtual ~Exception() throw ()
		{

		}

		boost::string_ref file()
		{
			return file_;
		}

		int line()
		{
			return line_;
		}

	protected:
		Exception(const boost::string_ref& file, int line, const std::string& what)
			: file_(file)
			, line_(line)
			, what_(what)
		{

		}

	private:
		const boost::string_ref file_;
		const int line_;
		const std::string what_;
	};


	// DataError is the parent of: DatabaseCorruptedException, IncompatibleDatabaseVersion, InternalErrorException.
	// DataError covers most cases where the database cannot be read because it is damaged or incompatible.
	class DataError : public Exception
	{
	public:
		DataError(const boost::string_ref& file, int line, const std::string& what)
			: Exception(file, line, what)
		{

		}
	};

	// InvalidArgumentException is thrown when a public API function receives an invalid argument. It is also thrown for client errors such as using a closed database.
	class InvalidArgumentException : public Exception
	{
	public:
		InvalidArgumentException(const boost::string_ref& file, int line, const boost::string_ref&, const std::string& message)
			: Exception(file, line, "Invalid argument: " + message + ".")
		{

		}
	};

	// DatabaseCorruptedException is thrown when unexpected data is found in the database.
	class DatabaseCorruptedException : public DataError
	{
	public:
		DatabaseCorruptedException(const boost::string_ref& file, int line, const boost::string_ref&, const std::string& message)
			: DataError(file, line, "Database corrupted: " + message + ".")
		{

		}
	};

	// IncompatibleDatabaseVersion is thrown when attempting to open a database with an incompatible/unsupported on-disk format.
	class IncompatibleDatabaseVersion : public DataError
	{
	public:
		IncompatibleDatabaseVersion(const boost::string_ref& file, int line, const boost::string_ref&, const std::string& message)
			: DataError(file, line, "Incompatible database version: " + message + ".")
		{

		}
	};

	// ValueTooLarge is thrown when attempting to store or retrieve a value larger than a limit configured on database open.
	class ValueTooLarge : public Exception
	{
	public:
		ValueTooLarge(const boost::string_ref& file, int line, const boost::string_ref&, const std::string& message)
			: Exception(file, line, "Value too large: " + message + ".")
		{

		}
	};

	// IoException is thrown on I/O errors.
	class IoException : public Exception
	{
	public:
		IoException(const boost::string_ref& file, int line, const boost::string_ref&, const std::string& message)
			: Exception(file, line, "I/O error: " + message + ".")
		{

		}
	};

	// NotYetImplementedException is thrown for functionality which is not yet implemented.
	class NotYetImplementedException : public Exception
	{
	public:
		NotYetImplementedException(const boost::string_ref& file, int line, const boost::string_ref& function, const std::string& message)
			: Exception(file, line, "Not yet implemented: " + message + " (in function " + function.to_string() + ").")
		{

		}
	};

	// InternalErrorException is thrown in all other cases.
	class InternalErrorException : public DataError
	{
	public:
		InternalErrorException(const boost::string_ref& file, int line, const boost::string_ref&, const std::string& message)
			: DataError(file, line, "Internal error:" + message + ".")
		{

		}
	};

}; // namespace hashdb
}; // namespace kerio
