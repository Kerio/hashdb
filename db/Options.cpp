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

// Options.cpp - implementation of Options ctor and methods.
#include "stdafx.h"
#include "utils/ExceptionCreator.h"
#include "utils/ConfigUtils.h"
#include "utils/MurmurHash3Adapter.h"
#include "utils/NullLogger.h"
#include <kerio/hashdb/Constants.h>
#include <kerio/hashdb/Options.h>

namespace kerio {
namespace hashdb {

	//-------------------------------------------------------------------------
	// Ctor and factory methods.

	Options::Options() 
		: createIfMissing_(true)
		, readOnly_(false)
		, pageSize_(defaultPageSize())
		, memoryPoolBytes_(defaultCacheBytes())
		, initialBuckets_(1)
		, hashFun_(murmur3Hash)
		, leavePageFreeSpace_(0)
		, largeValuesPerKey_(1)
		, minFlushFrequency_(20)
		, storeThrowIfLargerThan_(20 * 1024 * 1024)
		, fetchIgnoreIfLargerThan_(50 * 1024 * 1024)
		, lockManagerType_(NullLockManagerType)
		, pageAllocatorType_(SingleThreadedPageAllocatorType)
	{

	}

	kerio::hashdb::Options Options::readOnlySingleThreaded(boost::shared_ptr<ILogger> logger)
	{
		Options options;
		options.createIfMissing_ = false;
		options.readOnly_ = true;
		options.logger_ = logger;
		return options;
	}

	Options Options::readOnlySingleThreaded()
	{
		boost::shared_ptr<ILogger> logger(new NullLogger());
		return readOnlySingleThreaded(logger);
	}

	kerio::hashdb::Options Options::readWriteSingleThreaded(boost::shared_ptr<ILogger> logger)
	{
		Options options;
		options.logger_ = logger;
		return options;
	}

	Options Options::readWriteSingleThreaded()
	{
		boost::shared_ptr<ILogger> logger(new NullLogger());
		return readWriteSingleThreaded(logger);
	}

	void Options::validate() const
	{
		RAISE_INVALID_ARGUMENT_IF(createIfMissing_ && readOnly_, "Options: createIfMissing_ and readOnly_ cannot be both true");
		RAISE_INVALID_ARGUMENT_IF(! isValidPageSize(pageSize_),  "Options: page size pageSize_ is invalid (%u)", pageSize_);
		RAISE_INVALID_ARGUMENT_IF(initialBuckets_ == 0,          "Options: initial number of buckets initialBuckets_ must be greater than 0");
		RAISE_INVALID_ARGUMENT_IF(hashFun_ == NULL,              "Options: hash function pointer hashFun_ must not be null");
		RAISE_INVALID_ARGUMENT_IF(leavePageFreeSpace_ >= static_cast<int32_t>(pageSize_),
																 "Options: leavePageFreeSpace_ must be smaller than page size");
		RAISE_INVALID_ARGUMENT_IF(storeThrowIfLargerThan_ != 0 && storeThrowIfLargerThan_  <= MAX_PAGE_SIZE,  
																 "Options: storeThrowIfLargerThan_ must be either 0 or it must be larger than maximum page size");
		RAISE_INVALID_ARGUMENT_IF(fetchIgnoreIfLargerThan_ != 0 && fetchIgnoreIfLargerThan_ <= MAX_PAGE_SIZE,  
																 "Options: fetchIgnoreIfLargerThan_ must be either 0 or it must be larger than maximum page size");

		switch (lockManagerType_) {
		case NullLockManagerType:
		case TrueLockManagerType:
			break;

		default:
			RAISE_INVALID_ARGUMENT("Options: unknown lock manager type %u", lockManagerType_);
			break;
		}

		switch (pageAllocatorType_) {
		case SimplePageAllocatorType:
		case LockFreePageAllocatorType:
			break;

		case SingleThreadedPageAllocatorType:
			RAISE_INVALID_ARGUMENT_IF(lockManagerType_ == TrueLockManagerType, "Invalid options: SingleThreadedPageAllocatorType cannot be used in multithreaded environments");
			break;

		default:
			RAISE_INVALID_ARGUMENT("Invalid options: unknown page allocator type %u", pageAllocatorType_);
			break;
		}

		RAISE_INVALID_ARGUMENT_IF(! logger_, "Invalid options: logger pointer must not be null");
	}


	uint32_t Options::computeTestHash() const
	{
		RAISE_INTERNAL_ERROR_IF_ARG(hashFun_ == NULL);

		const char testData[] = "ZaUXuk&jZC1_KUZkSrXsIvOA";
		const uint32_t hash = hashFun_(testData, sizeof(testData));
		return hash;
	}

}; // namespace hashdb
}; // namespace kerio
