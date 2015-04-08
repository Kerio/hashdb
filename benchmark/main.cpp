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
#include <fstream>
#include "utils/ExceptionCreator.h"
#include "BenchmarkOptions.h"
#include "Runner.h"

using namespace kerio::hashdb::benchmark;

int main(int argc, char** argv) 
{
	try {

		if (argc < 2) {
			std::cout << BenchmarkOptions::usage() << std::endl;
		}

		BenchmarkOptions options(argc, argv);
		std::cout << options.list() << std::endl;

#if defined _DEBUG
		std::cout << "WARNING: Benchmark is compiled in debug mode" << std::endl;
#elif defined(__GNUC__) && ! defined(__OPTIMIZE__)
		std::cout << "WARNING: Benchmark optimization is disabled" << std::endl;
#elif ! defined NDEBUG
		std::cout << "WARNING: Assertions are enabled" << std::endl;
#endif
		
		const double seconds = (options.numberOfThreads_ == 1)? singleThreadedRunner(options) : multiThreadedRunner(options);
		std::cout << "* Finished in " << seconds << " seconds" << std::endl;

		// Report result in CSV format.
		if (! options.disableResultsCsv_) {
			const char* csvFileName = "results.csv";

			std::string csvLine = options.csv(seconds);
			std::cout << "* " << csvFileName << ": " << csvLine << std::endl;

			std::fstream fs;
			fs.open(csvFileName, std::fstream::out | std::fstream::app);
			fs << csvLine << std::endl;
			fs.close();
		}
	}
	catch (kerio::hashdb::Exception& e) {
		std::cerr << e.what() << std::endl;
	}
	catch (std::exception& e) {
		std::cerr << "Benchmark terminated by exception: " << e.what() << std::endl;
	}

}
