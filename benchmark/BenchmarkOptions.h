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
#pragma once
#include <limits>
#include <kerio/hashdb/Types.h>
#include "DatabaseWrapper.h"
#include "Benchmark.h"
#include "Sequence.h"
#include "DataProducer.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	struct BenchmarkOptions {
		static std::string usage();

		BenchmarkOptions(int argc, char** argv);

		// Runner options
		DatabaseWrapperFactory databaseWrapperFactory_;
		BenchmarkFactory benchmarkFactory_;
		size_type numberOfThreads_;			// --threads=N

		// Benchmark parameters
		SequenceFactory sequenceFactory_;	// --sequence={random|simple} --num=N --max=N
		DataProducer keyProducer_;			// --key={int|str}
		DataProducer valueProducer_;		// --value={int|str}

		// Output.
		bool verbose_;						// --verbose
		bool hashdbLog_;					// --hashdbLog

		bool disableResultsCsv_;			// --disable-csv
		std::string csvLabel_;				// --label=str

		// Database options
		std::string databaseName_;			// --db=str
		bool createNew_;					// --new
		size_type pageSize_;				// --pageSize=N
		size_type cacheSize_;				// --cache=N
		size_type initialBuckets_;			// --initialBuckets=N

		// HashDB-specific options
		bool hashdbPrefetch_;				// --hashdbPrefetchBytes
		int32_t hashdbPageFreeSpace_;		// --hashdbPageFreeSpace=N
		size_type hashdbFlushFrequency_;	// --hashdbFlushFrequency=N
		size_type hashdbBatchSize_;			// --hashdbBatchSize=N

		// BDB hash specific options
		size_type bdbHashFfactor_;			// --hash-ffactor=N

		// Constants
		static const size_type SIZE_VALUE_NOT_SET = 0xffffffff;

		std::string list() const;
		std::string csv(double seconds) const;

	private:
		void listValueOrNothing(std::ostringstream& os, const char* name, size_type value,  const char* unit = "") const;
		void listValueOrNothing(std::ostringstream& os, const char* name, int32_t value,  const char* unit = "") const;
	};

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
