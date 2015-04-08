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

// HashDB.h - interface to the hashdb.
//
// The hashdb database is a simple key/value store inspired by BerkeleyDB 1.x hashed file database.
// The key to hashdb is a string key which is used to compute the hash value, and a small integer 
// called partNum which allows several values related to the same key to be stored near each other 
// in the file.
//
// There are two types of API to the database:
// - simple API which can be used to store and fetch string values,
// - batch API which accepts batch classes created by the user to perform batch fetch, store or delete operations.
//
// Note that the batch retrieval can be made more efficient than retrieval by using the simple API, and that the simple 
// API itself is implemented by using the batch API.

#pragma once

#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/utility/string_ref.hpp>
#include <boost/filesystem/path.hpp>
#include <kerio/hashdb/Options.h>
#include <kerio/hashdb/Types.h>
#include <kerio/hashdb/BatchApi.h>
#include <kerio/hashdb/Iterator.h>
#include <kerio/hashdb/Statistics.h>

namespace kerio {
namespace hashdb {

	class IDatabase
	{
		//------------------------------------------------------------------------
		// Open and close.
	public:
		// Opens the database with create/open options passed in the options parameter.
		//
		// Each database comprises several database files. The database creates the actual file name 
		// of each database file by appending an extension to to the database name specified as a parameter.
		//
		// A database must not be open multiple times simultaneously.
		//
		// Example: db->open("filename", Options::readWriteSingleThreaded());
		virtual void open(const boost::filesystem::path& database, const Options& options) = 0;

		// Closes the database.
		virtual void close() = 0;

		//------------------------------------------------------------------------
		// Explicit buffer management.
	public:
		// Write all modified pages.
		virtual void flush() = 0;

		// Force all modified pages to disk
		virtual void sync() = 0;

		// Releases some resources held by this instance, e.g. when returning an open database to a database pool.
		virtual void releaseSomeResources() = 0;

		// Prefetch database pages. Calling this method makes sense only when cold-starting the application.
		virtual void prefetch() = 0;

		//------------------------------------------------------------------------
		// Simple API.
	public:
		// Fetches the value associated with the given key and partNum from the database.
		// If the value is found, the method sets the 'value' output parameter and returns true.
		// Otherwise the method returns false.
		virtual bool fetch(const boost::string_ref& key, partNum_t partNum, std::string& value) = 0;

		// Stores a record containing the given key, partNum, and value into the database.
		virtual	void store(const boost::string_ref& key, partNum_t partNum, const boost::string_ref& value) = 0;
		
		// Removes a record containing the given key and partNum from the database. 
		// The method does nothing if the record is not found.
		virtual void remove(const boost::string_ref& key, partNum_t partNum) = 0;

		// Removes all records containing the given key from the database.
		// The method does nothing if no such record is found.
		virtual void remove(const boost::string_ref& key) = 0;

		// Returns a list of all partNums associated with the given key in the database.
		virtual std::vector<partNum_t> listParts(const boost::string_ref& key) = 0;

		//------------------------------------------------------------------------
		// Batch API.
	public:
		// Performs a batch read. Returns true if all values were found and successfully set.
		virtual bool fetch(IReadBatch& readBatch) = 0;

		// Performs a batch write.
		virtual void store(const IWriteBatch& writeBatch) = 0;

		// Performs a batch delete.
		virtual void remove(const IDeleteBatch& deleteBatch) = 0;

		//------------------------------------------------------------------------
		// Iterator API.
	public:
		// Creates a new iterator over the database. Note that there can be only a single
		// iterator at a time.
		virtual Iterator newIterator() = 0;

		//------------------------------------------------------------------------
		// Statistics.
	public:
		// Retrieves database statistics.
		virtual Statistics statistics() = 0;

		//------------------------------------------------------------------------
		// Management of database files.
	public:
		// Returns true of database exists.
		virtual bool exists(const boost::filesystem::path& database) = 0;

		// Renames database files. Returns true if the source database files existed and were successfully renamed to target database files.
		virtual bool rename(const boost::filesystem::path& source, const boost::filesystem::path& target) = 0;

		// Deletes database files. Returns true if the database files existed and were successfully deleted.
		virtual bool drop(const boost::filesystem::path& database) = 0;

	public:
		virtual ~IDatabase() { }
	};

	typedef boost::shared_ptr<IDatabase> Database;

	Database DatabaseFactory();

}; // namespace hashdb
}; // namespace kerio
