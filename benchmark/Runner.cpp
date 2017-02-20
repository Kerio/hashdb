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
#include <kerio/hashdb/Exception.h>
#include "utils/ExceptionCreator.h"
#include "utils/StopWatch.h"
#include "BenchmarkOptions.h"
#include "Benchmark.h"

#if defined _WIN32
#include <process.h>
#else
#include <pthread.h>
#endif

namespace kerio {
namespace hashdb {
namespace benchmark {

	double singleThreadedRunner(const BenchmarkOptions& options)
	{
		Benchmark benchmark = options.benchmarkFactory_->newBenchmark();

		StopWatch stopWatch;
		benchmark->prepare(options);
		benchmark->run();
		return stopWatch.seconds();
	}

	//----------------------------------------------------------------------------

#if defined _WIN32

	unsigned __stdcall startHelper(void*);

	class BenchmarkThreadContainer {
	public:
		BenchmarkThreadContainer(Benchmark benchmark)
			: benchmark_(benchmark)
			, threadHandle_(0)
		{
		}

		void runInThread()
		{
			std::cout << "*** Starting thread for benchmark instance " << benchmark_->instance() << std::endl;
			threadHandle_ = reinterpret_cast<HANDLE>(_beginthreadex(NULL, 0, &startHelper, this, 0, NULL));
			if (threadHandle_ == 0) {
				char buffer[100];
				strerror_s(buffer, errno);

				printf("ERROR: _beginthreadex returned 0 for benchmark instance %u: %s\n", benchmark_->instance(), buffer);
				exit(1);
			}
		}

		void prepareBenchmark(const BenchmarkOptions& options)
		{
			benchmark_->prepare(options);
		}

		void runBenchmark()
		{
			benchmark_->run();
		}

		void waitFor()
		{
			WaitForSingleObject(threadHandle_, INFINITE);
			CloseHandle(threadHandle_);
			threadHandle_ = 0;
		}

	private:
		Benchmark benchmark_;
		HANDLE threadHandle_;
	};

	unsigned __stdcall startHelper(void* startArg) 
	{
		BenchmarkThreadContainer* workerPtr = static_cast<BenchmarkThreadContainer*>(startArg);
		workerPtr->runBenchmark();
		return 0;
	}

#else

	void *startHelper(void *);

	class BenchmarkThreadContainer {
	public:
		BenchmarkThreadContainer(Benchmark benchmark)
			: benchmark_(benchmark)
		{
		}

		void runInThread()
		{
			std::cout << "*** Starting thread for benchmark instance " << benchmark_->instance() << std::endl;
			const int err = pthread_create(&threadHandle_, NULL, &startHelper, this);
			if (err) {
				printf("ERROR: pthread_create returned %u benchmark instance %u\n", err, benchmark_->instance());
				exit(1);
			}
		}

		void prepareBenchmark(const BenchmarkOptions& options)
		{
			benchmark_->prepare(options);
		}

		void runBenchmark()
		{
			benchmark_->run();
		}

		void waitFor()
		{
			const int err = pthread_join(threadHandle_, NULL);
			if (err) {
				printf("ERROR: pthread_join returned %u benchmark instance %u\n", err, benchmark_->instance());
				exit(1);
			}
		}

	private:
		Benchmark benchmark_;
		pthread_t threadHandle_;
	};

	void* startHelper(void* startArg) 
	{
		BenchmarkThreadContainer* workerPtr = static_cast<BenchmarkThreadContainer*>(startArg);
		workerPtr->runBenchmark();
		return NULL;
	}

#endif // defined _WIN32

	double multiThreadedRunner(const BenchmarkOptions& options)
	{
		typedef std::vector<BenchmarkThreadContainer> workersVector_t;
		workersVector_t workers;

		for (size_type i = 1; i <= options.numberOfThreads_; ++i) {
			Benchmark newBenchmark = options.benchmarkFactory_->newBenchmark(i);
			workers.push_back(BenchmarkThreadContainer(newBenchmark));
		}

		StopWatch stopWatch;
		
		for (workersVector_t::iterator ii = workers.begin(); ii != workers.end(); ++ii) {
			ii->prepareBenchmark(options);
		}

		std::cout << "* All workers are prepared" << std::endl;

		for (workersVector_t::iterator ii = workers.begin(); ii != workers.end(); ++ii) {
			ii->runInThread();
		}

		for (workersVector_t::iterator ii = workers.begin(); ii != workers.end(); ++ii) {
			ii->waitFor();
		}

		return stopWatch.seconds();
	}

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
