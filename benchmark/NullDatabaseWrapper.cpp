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
#include "utils/ExceptionCreator.h"
#include "BenchmarkOptions.h"
#include "BenchmarkException.h"
#include "NullDatabaseWrapper.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	class NullDatabaseWrapper : public IDatabaseWrapper
	{
	public:
		NullDatabaseWrapper(const BenchmarkOptions& benchmarkOptions)
		{
			verbose_ = benchmarkOptions.verbose_;
		}

		virtual void checkedFetch(size_type number, const std::string& key, const std::string& expectedValue)
		{
			if (verbose_) {
				std::cout << "checkedFetch(" << number << ", \"";
				printJsonString(std::cout, key);
				std::cout << "\", \"";
				printJsonString(std::cout, expectedValue);
				std::cout << "\")" << std::endl;
			}

			std::string actualValue = expectedValue;

			if (expectedValue != actualValue) {
				RAISE_BENCHMARK_EXCEPTION("unexpected value for key number %u", number);
			}
		}

		virtual	void store(const std::string& key, const std::string& value)
		{
			if (verbose_) {
				std::cout << "store(\"";
				printJsonString(std::cout, key);
				std::cout << "\", \"";
				printJsonString(std::cout, value);
				std::cout << "\")" << std::endl;
			}
		}

		virtual void remove(const std::string& key)
		{
			if (verbose_) {
				std::cout << "remove(\"";
				printJsonString(std::cout, key);
				std::cout << "\")" << std::endl;
			}
		}

		virtual void close()
		{
		}

	private:
		bool verbose_;
	};


	class NullDatabaseWrapperFactory : public IDatabaseWrapperFactory {
	public:
		virtual std::string name()
		{
			return "null";
		}

		virtual DatabaseWrapper open(const BenchmarkOptions& benchmarkOptions, size_type instanceNumber)
		{
			if (benchmarkOptions.verbose_) {
				std::cout << "Opening null database instance number " << instanceNumber << std::endl;
			}
			DatabaseWrapper newInstance(new NullDatabaseWrapper(benchmarkOptions));
			return newInstance;
		}
	};

	DatabaseWrapperFactory newNullDatabaseWrapperFactory()
	{
		DatabaseWrapperFactory newInstance(new NullDatabaseWrapperFactory());
		return newInstance;
	}


}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
