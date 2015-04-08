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
#include <kerio/hashdb/HashDB.h>
#include <kerio/hashdb/Constants.h>
#include <kerio/hashdb/Exception.h>
#include "utils/ConfigUtils.h"
#include "db/SingleThreadedPageAllocator.h"
#include "testUtils/TestPageAllocator.h"
#include "SingleThreadedPageAllocatorTest.h"

#include "db/Vector.h"
#include "db/OverflowDataPage.h"

using namespace kerio::hashdb;

void SingleThreadedPageAllocatorTest::testInvalidRequest()
{
	const size_type pageSize = MIN_PAGE_SIZE;
	const size_type cachePages = 5;
	const size_type cacheSize = pageSize * cachePages;

	boost::scoped_ptr<IPageAllocator> allocator(new SingleThreadedPageAllocator(cacheSize));
	TS_ASSERT_THROWS(allocator->allocate(0), InternalErrorException);
	TS_ASSERT_THROWS(allocator->allocate(333), InternalErrorException);
	TS_ASSERT_THROWS(allocator->deallocate(NULL, NULL), InternalErrorException);

	{
		IPageAllocator::PageMemoryPtr ptr = allocator->allocate(pageSize); // All subsequent allocations must use pageSize.
	}

	{
		TS_ASSERT_THROWS(allocator->allocate(defaultPageSize()), InternalErrorException);
	}

	TS_ASSERT_EQUALS(1U, allocator->heldPages());
}

void SingleThreadedPageAllocatorTest::testAllocate()
{
	const size_type pageSize = MIN_PAGE_SIZE;
	const size_type cachePages = 5;
	const size_type cacheSize = pageSize * cachePages;
	const size_type testMaxPages = 10;

	boost::scoped_ptr<IPageAllocator> allocator(new SingleThreadedPageAllocator(cacheSize));

	std::vector<IPageAllocator::PageMemoryPtr> allocatedEntries;

	// Allocate testMaxPages pages.
	for (size_type i = 0; i < testMaxPages; ++i) {
		allocatedEntries.push_back(allocator->allocate(pageSize));
		TS_ASSERT_EQUALS(0U, allocator->heldPages());
	}

	// Allocate and free a page.
	{
		IPageAllocator::PageMemoryPtr mem = allocator->allocate(pageSize);
	}
	TS_ASSERT_EQUALS(1U, allocator->heldPages());

	// Fill the cache.
	for (size_type i = 0; i < cachePages - 1; ++i) {
		allocatedEntries.erase(allocatedEntries.begin());
		TS_ASSERT_EQUALS(i + 2, allocator->heldPages());
	}

	// Release remaining pages.
	while (! allocatedEntries.empty()) {
		TS_ASSERT_THROWS_NOTHING(allocatedEntries.erase(allocatedEntries.begin()));
		TS_ASSERT_EQUALS(cachePages, allocator->heldPages());
	}
	
	// Allocate and free 3 pages.
	for (size_type i = 0; i < 3; ++i) {
		allocatedEntries.push_back(allocator->allocate(pageSize));
		TS_ASSERT_EQUALS(cachePages - i - 1, allocator->heldPages());
	}

	allocatedEntries.clear();
	TS_ASSERT_EQUALS(cachePages, allocator->heldPages());

	// Allocate cachePages pages.
	for (size_type i = 0; i < cachePages; ++i) {
		allocatedEntries.push_back(allocator->allocate(pageSize));
		TS_ASSERT_EQUALS(cachePages - i - 1, allocator->heldPages());
	}

	// Allocate up to testMaxPages pages.
	for (size_type i = 0; i < testMaxPages - cachePages; ++i) {
		allocatedEntries.push_back(allocator->allocate(pageSize));
		TS_ASSERT_EQUALS(0U, allocator->heldPages());
	}

	// Free all pages.
	allocatedEntries.clear();
	TS_ASSERT_EQUALS(cachePages, allocator->heldPages());
}
