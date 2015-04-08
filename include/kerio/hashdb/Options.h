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

// Options.h - hashdb create/open database options and associated interfaces.
#pragma once
#include <boost/shared_ptr.hpp>
#include <kerio/hashdb/Types.h>

namespace kerio {
namespace hashdb {

	//-------------------------------------------------------------------------
	// Logger interface to be implemented in hashdb clients.

	class ILogger
	{
	public:
		// Returns true if debug log is enabled, otherwise returns false.
		virtual bool isDebugLogEnabled() = 0;

		// Logs a debug message.
		virtual void logDebug(const std::string& message) = 0;

		virtual ~ILogger() { }
	};

	//-------------------------------------------------------------------------
	// Options for database open.

	struct Options { // intentionally copyable

		// Open database read-only in a single-threaded environment.
		static Options readOnlySingleThreaded(boost::shared_ptr<ILogger> logger);
		static Options readOnlySingleThreaded();
		
		// Open database read-write in a single-threaded environment.
		static Options readWriteSingleThreaded(boost::shared_ptr<ILogger> logger);
		static Options readWriteSingleThreaded();	

		void validate() const;
		uint32_t computeTestHash() const;

		typedef uint32_t (*hashFun_t)(const char* key, size_t len);

		bool createIfMissing_;				// Database is created if not found. The default for R/W instances is "true".
		bool readOnly_;						// Database files are opened read only. The default for R/W instances is  "false".
		size_type pageSize_;				// Page size, must be a power of 2 between MIN_PAGE_SIZE and MAX_PAGE_SIZE. Default is 4K (sector size of 1st gen Advanced Format HDD's).
		size_type memoryPoolBytes_;			// Private memory pool bytes to be kept by the instance's page allocator. Default is 16K.
		size_type initialBuckets_;			// Initial number of buckets when a new database is created. Default is 1.
		hashFun_t hashFun_;					// Hash function. Default is murmur3Hash (adapter for MurmurHash3).
		int32_t leavePageFreeSpace_;		// Positive or negative correction to the computed fill factor used for performance testing. Default is 0.
		size_type largeValuesPerKey_;		// Number of large value parts expected to be stored for a single key. Default is 1.
		size_type minFlushFrequency_;		// Minimum number of write requests after which the metadata is flushed. Default is 20.

		// Store and fetch limits.
		size_type storeThrowIfLargerThan_;	// Attempt to store a value larger than the limit causes exception ValueTooLarge (0 means no limit). The default limit is 20 MB.
		size_type fetchIgnoreIfLargerThan_;	// Attempt to fetch a value larger than the limit fails as if the value did not exist (0 means no limit). Default limit is 50 MB.

		enum LockManager_t
		{
			NullLockManagerType,			// "Null" lock manager that does nothing.
			TrueLockManagerType				// Actual implementation of the lock manager interface.
		};
		LockManager_t lockManagerType_;		// Lock manager type. The default for single-threaded instances is NullLockManagerType.

		enum PageAllocator_t
		{
			SimplePageAllocatorType,		// Simple malloc-based page allocator.
			SingleThreadedPageAllocatorType,// Efficient single-threaded page allocator.
			LockFreePageAllocatorType		// Lock-free page allocator for multi-threaded environments.
		};
		PageAllocator_t pageAllocatorType_; // Page allocator type. The default for single-threaded instances is SingleThreadedPageAllocatorType.

		boost::shared_ptr<ILogger> logger_; // Logger. Default is NullLogger.

	private:
		Options();
	};

}; // namespace hashdb
}; // namespace kerio
