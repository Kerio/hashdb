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
#include <kerio/hashdb/Constants.h>
#include "testUtils/TestPageAllocator.h"
#include "testUtils/StringUtils.h"
#include "db/LargeValuePage.h"
#include "LargeValuePageTest.h"

using namespace kerio::hashdb;

LargeValuePageTest::LargeValuePageTest()
	: allocator_(new TestPageAllocator())
{

}

namespace {

	void doTestSetUpAndValidateLargeValuePage(IPageAllocator* allocator, size_type pageSize)
	{
		LargeValuePage page(allocator, pageSize);

		page.setUp(1234);
		TS_ASSERT_THROWS_NOTHING(page.validate());

		page.clearDirtyFlag();
	}

};

void LargeValuePageTest::testSetUpAndValidateLargeValuePage()
{
	TS_ASSERT_THROWS_NOTHING(doTestSetUpAndValidateLargeValuePage(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestSetUpAndValidateLargeValuePage(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}

namespace {

	size_type exactValueForPageSize(size_type pageSize)
	{
		return pageSize - 16;
	}

	size_type doTestAddValue(IPageAllocator* allocator, size_type pageSize, size_type valueSize)
	{
		size_type pagesInvolved = 0;

		LargeValuePage page1(allocator, pageSize);
		page1.setUp(1234);
		TS_ASSERT_THROWS_NOTHING(page1.validate());

		LargeValuePage page2(allocator, pageSize);
		page2.setUp(1235);
		TS_ASSERT_THROWS_NOTHING(page2.validate());

		const std::string storedValue = valueOfSize(valueSize);
		size_type putPosition = 0;
		
		if (page1.putValuePart(putPosition, storedValue)) {
			page1.setNextLargeValuePage(1235);
			TS_ASSERT(! page2.putValuePart(putPosition, storedValue));
		}

		TS_ASSERT_EQUALS(putPosition, storedValue.size());

		std::string fetchedValue;
		if (page1.getValuePart(fetchedValue, valueSize)) {
			TS_ASSERT_EQUALS(page1.getNextLargeValuePage(), 1235U);
			TS_ASSERT(! page2.getValuePart(fetchedValue, valueSize));
			pagesInvolved = 2;
		}
		else {
			TS_ASSERT_EQUALS(page1.getNextLargeValuePage(), 0U);
			pagesInvolved = 1;
		}

		TS_ASSERT_EQUALS(storedValue, fetchedValue);

		page1.clearDirtyFlag();
		page2.clearDirtyFlag();

		return pagesInvolved;
	}

};

void LargeValuePageTest::testAddBiggerValue()
{
	TS_ASSERT_EQUALS(2U, doTestAddValue(allocator_.get(), MIN_PAGE_SIZE, exactValueForPageSize(MIN_PAGE_SIZE) + 1));
	TS_ASSERT_EQUALS(2U, doTestAddValue(allocator_.get(), MAX_PAGE_SIZE, exactValueForPageSize(MAX_PAGE_SIZE) + 1));
	TS_ASSERT(allocator_->allFreed());
}

void LargeValuePageTest::testAddExactValue()
{
	TS_ASSERT_EQUALS(1U, doTestAddValue(allocator_.get(), MIN_PAGE_SIZE, exactValueForPageSize(MIN_PAGE_SIZE)));
	TS_ASSERT_EQUALS(1U, doTestAddValue(allocator_.get(), MAX_PAGE_SIZE, exactValueForPageSize(MAX_PAGE_SIZE)));
	TS_ASSERT(allocator_->allFreed());
}

void LargeValuePageTest::testSingleByteValue()
{
	TS_ASSERT_EQUALS(1U, doTestAddValue(allocator_.get(), MIN_PAGE_SIZE, 1));
	TS_ASSERT_EQUALS(1U, doTestAddValue(allocator_.get(), MAX_PAGE_SIZE, 1));
	TS_ASSERT(allocator_->allFreed());
}
