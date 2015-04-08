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
#include "Sequence.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	struct BenchmarkOptions;

	class IBenchmark {
	public:
		virtual void prepare(const BenchmarkOptions& options) = 0;
		virtual void run() = 0;
		virtual size_type instance() = 0;
		virtual ~IBenchmark() { };
	};

	typedef boost::shared_ptr<IBenchmark> Benchmark;

	class IBenchmarkFactory {
	public:
		virtual Benchmark newBenchmark(size_type instanceNumber = 0) = 0;
		virtual std::string name() const = 0;
		virtual bool isBatch() = 0;
		virtual SequenceFactory defaultSequenceFactory() = 0;
		virtual ~IBenchmarkFactory() { }

	protected:
		static const size_type DEFAULT_WRITE_ITERATIONS = 200000;
		static const size_type DEFAULT_READ_ITERATIONS  = 100000;
		static const size_type DEFAULT_UPDATE_ITERATIONS =  5000;
		static const size_type DEFAULT_DELETE_ITERATIONS =  5000;
	};

	typedef boost::shared_ptr<IBenchmarkFactory> BenchmarkFactory;

	// Single entry put/get/delete.
	BenchmarkFactory newWriteBenchmarkFactory();
	BenchmarkFactory newUpdateBenchmarkFactory();
	BenchmarkFactory newReadBenchmarkFactory();
	BenchmarkFactory newDeleteBenchmarkFactory();

	// Batch access - HashDB only.
	BenchmarkFactory newBatchWriteBenchmarkFactory();
	BenchmarkFactory newBatchReadBenchmarkFactory();

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
