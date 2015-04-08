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
#include "stdafx.h"
#include <iostream>
#include <kerio/hashdb/HashDB.h>
#include <kerio/hashdb/Exception.h>
#include <kerio/hashdbHelpers/StdoutLogger.h>
#include "utils/ExceptionCreator.h"
#include "BenchmarkOptions.h"
#include "BenchmarkException.h"
#include "HashdbWrapper.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	HashdbWapper::HashdbWapper(const BenchmarkOptions& benchmarkOptions, size_type instanceNumber)
	{
		database_ = DatabaseFactory();

		if (benchmarkOptions.createNew_) {
			database_->drop(benchmarkOptions.databaseName_);
		}

		kerio::hashdb::Options options = kerio::hashdb::Options::readWriteSingleThreaded();
			
		if (benchmarkOptions.pageSize_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
			options.pageSize_ = benchmarkOptions.pageSize_;
		}
			
		if (benchmarkOptions.cacheSize_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
			options.memoryPoolBytes_ = benchmarkOptions.cacheSize_;
		}

		if (benchmarkOptions.initialBuckets_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
			options.initialBuckets_ = benchmarkOptions.initialBuckets_;
		}

		if (benchmarkOptions.hashdbPageFreeSpace_ != 0) {
			options.leavePageFreeSpace_ = benchmarkOptions.hashdbPageFreeSpace_;
		}

		if (benchmarkOptions.hashdbFlushFrequency_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
			options.minFlushFrequency_ = benchmarkOptions.hashdbFlushFrequency_;
		}

		if (benchmarkOptions.hashdbLog_) {
			options.logger_.reset(new StdoutLogger);
		}

		std::string databaseName = benchmarkOptions.databaseName_;
		if (instanceNumber != 0) {
			databaseName += formatMessage("_%u", instanceNumber);
		}
		
		database_->open(databaseName, options);

		if (benchmarkOptions.hashdbPrefetch_) {
			database_->prefetch();
		}
	}

	void HashdbWapper::checkedFetch(size_type number, const std::string& key, const std::string& expectedValue)
	{
		std::string actualValue;

		if (! database_->fetch(key, 0, actualValue)) {
			RAISE_BENCHMARK_EXCEPTION("unable to fetch value for key number %u", number);
		}

		if (expectedValue != actualValue) {
			RAISE_BENCHMARK_EXCEPTION("unexpected value for key number %u", number);
		}
	}

	void HashdbWapper::store(const std::string& key, const std::string& value)
	{
		database_->store(key, 0, value);
	}

	void HashdbWapper::remove(const std::string& key)
	{
		database_->remove(key);
	}

	void HashdbWapper::close()
	{
		database_->close();
		database_.reset();
	}

	Database HashdbWapper::database()
	{
		return database_;
	}

	
	class HashdbWrapperFactory : public IDatabaseWrapperFactory {
	public:
		virtual std::string name()
		{
			return "hashdb";
		}

		virtual DatabaseWrapper open(const BenchmarkOptions& benchmarkOptions, size_type instanceNumber)
		{
			DatabaseWrapper newInstance(new HashdbWapper(benchmarkOptions, instanceNumber));
			return newInstance;
		}
	};

	DatabaseWrapperFactory newHashdbWapperFactory()
	{
		DatabaseWrapperFactory newInstance(new HashdbWrapperFactory());
		return newInstance;
	}

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
