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
#include "utils/ConfigUtils.h"
#include "testUtils/TestPageAllocator.h"
#include "db/BitmapPage.h"
#include "BitmapPageTest.h"

using namespace kerio::hashdb;

BitmapPageTest::BitmapPageTest()
	: allocator_(new TestPageAllocator())
{

}

//-----------------------------------------------------------------------------

namespace {

	void doTestFreeNotAllocated(IPageAllocator* allocator, size_type pageSize)
	{
		BitmapPage bitmapPage(allocator, pageSize);

		bitmapPage.setUp(12345678);
		TS_ASSERT_THROWS_NOTHING(bitmapPage.validate());

		const size_type endOfPageOffsetRange = bitmapPage.numberOfManagedPages();
		TS_ASSERT(endOfPageOffsetRange < pageSize * 8);
		TS_ASSERT(endOfPageOffsetRange > 1000);

		TS_ASSERT_THROWS(bitmapPage.releasePage(0), DatabaseCorruptedException);
		TS_ASSERT_THROWS(bitmapPage.releasePage(33), DatabaseCorruptedException);
		TS_ASSERT_THROWS(bitmapPage.releasePage(endOfPageOffsetRange - 1), DatabaseCorruptedException&); // test catch reference
		TS_ASSERT_THROWS(bitmapPage.releasePage(endOfPageOffsetRange), const InternalErrorException&);   // test catch const reference
		TS_ASSERT_THROWS(bitmapPage.releasePage(endOfPageOffsetRange), const DataError&);   // test that InternalErrorException is inherited
		TS_ASSERT_THROWS(bitmapPage.releasePage(endOfPageOffsetRange), const Exception&);   // test that InternalErrorException is inherited

		TS_ASSERT(bitmapPage.dirty());
		bitmapPage.clearDirtyFlag();
	}

};

void BitmapPageTest::testFreeNotAllocated()
{
	TS_ASSERT_THROWS_NOTHING(doTestFreeNotAllocated(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestFreeNotAllocated(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}

//-----------------------------------------------------------------------------

void BitmapPageTest::testSingleAlloc()
{
	{
		BitmapPage bitmapPage(allocator_.get(), defaultPageSize());

		bitmapPage.setUp(12345678);
		TS_ASSERT_THROWS_NOTHING(bitmapPage.validate());

		// Allocate.
		TS_ASSERT_EQUALS(0U, bitmapPage.acquirePage());
		bitmapPage.releasePage(0);

		TS_ASSERT(bitmapPage.dirty());
		bitmapPage.clearDirtyFlag();
	}

	TS_ASSERT(allocator_->allFreed());
}

//-----------------------------------------------------------------------------

namespace {

	void doTestAllocateAll(IPageAllocator* allocator, size_type pageSize)
	{
		BitmapPage bitmapPage(allocator, pageSize);

		bitmapPage.setUp(12345678);
		TS_ASSERT_THROWS_NOTHING(bitmapPage.validate());

		const size_type endOfPageOffsetRange = bitmapPage.numberOfManagedPages();

		// Allocate.
		for (uint32_t expectedPageOffset = 0; expectedPageOffset < endOfPageOffsetRange; ++expectedPageOffset) {
			const uint32_t actualPageOffset = bitmapPage.acquirePage();

			TS_ASSERT_EQUALS(expectedPageOffset, actualPageOffset);
		}

		TS_ASSERT(bitmapPage.dirty());
		bitmapPage.clearDirtyFlag();

		// Check that page cannot be allocated.
		TS_ASSERT_EQUALS(BitmapPage::NO_SPACE, bitmapPage.acquirePage());
		TS_ASSERT(! bitmapPage.dirty());

		// Free the pages.
		for (uint32_t freedPageOffset = 0; freedPageOffset < endOfPageOffsetRange; ++freedPageOffset) {
			bitmapPage.releasePage(freedPageOffset);
		}

		TS_ASSERT(bitmapPage.dirty());
		bitmapPage.clearDirtyFlag();

		TS_ASSERT_THROWS(bitmapPage.releasePage(0), DatabaseCorruptedException);
		TS_ASSERT(! bitmapPage.dirty());

		// Allocate again (to check that free works correctly.
		for (uint32_t expectedPageOffset = 0; expectedPageOffset < endOfPageOffsetRange; ++expectedPageOffset) {
			const uint32_t actualPageOffset = bitmapPage.acquirePage();

			TS_ASSERT_EQUALS(expectedPageOffset, actualPageOffset);
		}

		TS_ASSERT(bitmapPage.dirty());

		// Free some of the pages.
		bitmapPage.releasePage(endOfPageOffsetRange - 1);
		bitmapPage.releasePage(33);
		bitmapPage.releasePage(32);

		TS_ASSERT_EQUALS(32U, bitmapPage.acquirePage());
		TS_ASSERT_EQUALS(33U, bitmapPage.acquirePage());
		TS_ASSERT_EQUALS(endOfPageOffsetRange - 1, bitmapPage.acquirePage());

		TS_ASSERT(bitmapPage.dirty());
		bitmapPage.clearDirtyFlag();
	}

};

void BitmapPageTest::testAllocateAll()
{
	TS_ASSERT_THROWS_NOTHING(doTestAllocateAll(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestAllocateAll(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}
