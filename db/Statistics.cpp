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

// Statistics.cpp - implementation of Statistics ctor and methods.
#include "stdafx.h"
#include <sstream>
#include <kerio/hashdb/Statistics.h>

namespace kerio {
namespace hashdb {

	Statistics::Statistics()
		: bucketPagesAcquired_(0)
		, overflowPagesAcquired_(0)
		, largeValuePagesAcquired_(0)
		, bitmapPagesAcquired_(0)
		, bucketPagesReleased_(0)
		, overflowPagesReleased_(0)
		, largeValuePagesReleased_(0)
		, bitmapPagesReleased_(0)
		, splitsOnOverfill_(0)
		, cachedPages_(0)
		, pageSize_(0)
		, numberOfBuckets_(0)
		, overflowFileDataPages_(0)
		, overflowFileBitmapPages_(0)
		, numberOfRecords_(0)
		, dataInlineSize_(0)
	{

	}

	void Statistics::printSessionStats(std::ostream& os)
	{
		os << "Bucket pages acquired: " << bucketPagesAcquired_ << std::endl;
		os << "Overflow pages acquired: " << overflowPagesAcquired_ << std::endl;
		os << "Large value pages acquired: " << largeValuePagesAcquired_ << std::endl;
		os << "Bitmap pages acquired: " << bitmapPagesAcquired_ << std::endl;

		os << "Bucket pages released: " << bucketPagesReleased_ << std::endl;
		os << "Overflow pages released: " << overflowPagesReleased_ << std::endl;
		os << "Overflow pages released: " << overflowPagesReleased_ << std::endl;
		os << "Bitmap pages released: " << bitmapPagesReleased_ << std::endl;

		os << "Splits on overfill: " << splitsOnOverfill_ << std::endl;
		os << "Cached pages: " << cachedPages_ << std::endl;
	}

	void Statistics::printDatabaseStats(std::ostream& os)
	{
		os << "Page size: " << pageSize_ << " bytes" << std::endl;
		os << "Number of buckets: " << numberOfBuckets_ << std::endl;
		os << "Overflow file data pages: " << overflowFileDataPages_ << std::endl;
		os << "Overflow file bitmap pages: " << overflowFileBitmapPages_ << std::endl;
		os << "Number of records: " << numberOfRecords_ << std::endl;
		os << "Data inline size: " << dataInlineSize_ << " bytes" << std::endl;
	}

	std::string Statistics::toString()
	{
		std::ostringstream os;

		printSessionStats(os);
		printDatabaseStats(os);

		return os.str();
	}

}; // namespace hashdb
}; // namespace kerio
