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

// BucketHeaderPage.cpp - bucket file header page.
#include "stdafx.h"
#include "BucketHeaderPage.h"

namespace kerio {
namespace hashdb {

	void BucketHeaderPage::validate() const
	{
		HeaderPage::validate();

		// HighestPageNumber must be HighestBucket + 1
		// 16     4    HighestPageNumber: highest page number in this file
		// 24     4    HighestBucket: highest bucket in use
		const uint32_t highestBucketNumber = getHighestBucket();
		const uint32_t highestPageNumber = getHighestPageNumber();
		RAISE_DATABASE_CORRUPTED_IF(highestPageNumber != highestBucketNumber + 1, "bad highest bucket number %u for bucket file with %u pages on %s", highestBucketNumber, highestPageNumber, getId().toString());

		// DataSize >= NumberOfRecords * minimum record size
		const uint64_t numberOfRecords = getDatabaseNumberOfRecords();
		const uint64_t dataSize = getDataSize();
		RAISE_DATABASE_CORRUPTED_IF(numberOfRecords * 5 > dataSize, "data size %u too small for %u records on %s", dataSize, numberOfRecords, getId().toString());
	}

}; // namespace hashdb
}; // namespace kerio
