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

#if defined _LINUX
#include <time.h>
#if ! defined CLOCK_MONOTONIC_RAW // Workaround for Debian 6
#define CLOCK_MONOTONIC_RAW		4
#endif // ! defined CLOCK_MONOTONIC_RAW
#elif defined _MACOS
#include <mach/mach.h>
#include <mach/mach_time.h>
#endif

#include <kerio/hashdb/Exception.h>
#include "ExceptionCreator.h"
#include "StopWatch.h"

namespace kerio {
namespace hashdb {

#if defined _WIN32

	class StopWatchImpl {
	public:
		StopWatchImpl()
		{
			GetSystemTime(&startTime_);
		}

		double seconds() const
		{
			SYSTEMTIME now;
			GetSystemTime(&now);

			FILETIME ftStart;
			RAISE_INTERNAL_ERROR_IF_ARG(! SystemTimeToFileTime(&startTime_, &ftStart));

			ULARGE_INTEGER ulStart;
			ulStart.HighPart = ftStart.dwHighDateTime;
			ulStart.LowPart = ftStart.dwLowDateTime;

			FILETIME ftNow;
			RAISE_INTERNAL_ERROR_IF_ARG(! SystemTimeToFileTime(&now, &ftNow));

			ULARGE_INTEGER ulNow;
			ulNow.HighPart = ftNow.dwHighDateTime;
			ulNow.LowPart = ftNow.dwLowDateTime;

			ULONGLONG difference = (ulNow.QuadPart - ulStart.QuadPart) / 10000;
			double seconds = difference / 1000.0;

			return seconds;
		}

	private:
		SYSTEMTIME startTime_;
	};

#elif defined _LINUX

	class StopWatchImpl {
	public:
		StopWatchImpl()
		{
			::clock_gettime(CLOCK_MONOTONIC_RAW, &startTime_);
		}

		double seconds()
		{
			struct timespec nowTime;
			::clock_gettime(CLOCK_MONOTONIC_RAW, &nowTime);

			const long long start = (startTime_.tv_sec * 1000) + (startTime_.tv_nsec / 1000000);
			const long long now = (nowTime.tv_sec * 1000) + (nowTime.tv_nsec / 1000000);
			const long long span = now - start;

			const double seconds = span / 1000.0;
			return seconds;
		}

	private:
		struct timespec startTime_;
	};

#elif defined _MACOS

	class StopWatchImpl {
	public:
		StopWatchImpl()
			: startTime_(::mach_absolute_time())
		{

		}

		double seconds()
		{
			const uint64_t nowTime = ::mach_absolute_time();
			const uint64_t span = nowTime - startTime_;
			
			mach_timebase_info_data_t sTimebaseInfo;
			mach_timebase_info(&sTimebaseInfo);
			
			const double seconds = span * (sTimebaseInfo.numer / 1000000000.0) / sTimebaseInfo.denom;
			return seconds;
		}

	private:
		const uint64_t startTime_;
	};


#endif

	StopWatch::StopWatch()
		: pimpl_(new StopWatchImpl())
	{

	}

	StopWatch::~StopWatch()
	{

	}

	double StopWatch::seconds() const
	{
		return pimpl_->seconds();
	}

}; // namespace hashdb
}; // namespace kerio
