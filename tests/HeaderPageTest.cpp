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
#include "db/BucketHeaderPage.h"
#include "db/OverflowHeaderPage.h"
#include "db/Version.h"
#include "HeaderPageTest.h"

using namespace kerio::hashdb;

HeaderPageTest::HeaderPageTest()
	: allocator_(new TestPageAllocator())
{

}

void HeaderPageTest::testDecodePage()
{
	static const size_type PAGE_SIZE = 64;
	uint8_t pageData[] = {
		0xc8, 0x75, 0x8d, 0x07, // 0: page type magic number - 0x078d75c8 = bucket file header
		0xaa, 0x55, 0x11, 0x22, // 4: checksum - 0x221155aa
		0, 0, 0, 0,				// 8: page number = 0
		0, 1, 0, 0,				// 12: database version = 0x100
		2, 2, 2, 2,				// 16: creation timestamp
		3, 3, 3, 3,				// 20: creation tag
		PAGE_SIZE, 0, 0, 0,		// 24: page size = PAGE_SIZE
		23, 0, 0, 0,			// 28: highest page number in this file = 23
		1, 2, 3, 4,				// 32: value of hash(testKey) = 0x04030201
		24, 0, 0, 0,			// 36: highest bucket in use = 24
		42, 0, 0, 0, 0, 0, 0, 0,// 40: number of records in the database
		0, 1, 2, 3, 4, 5, 6, 7	// 48: size of key/value data in the database
	};

	BucketHeaderPage page(allocator_.get(), PAGE_SIZE);
	page.clear();
	page.putBytes(0, boost::string_ref(reinterpret_cast<const char*>(&pageData[0]), sizeof(pageData)));
	page.clearDirtyFlag();

	TS_ASSERT_EQUALS(page.getMagic(), 0x078d75c8U);
	TS_ASSERT_EQUALS(page.getChecksum(), 0x221155aaU);
	TS_ASSERT_EQUALS(page.getPageNumber(), 0U);
	TS_ASSERT_EQUALS(page.getDatabaseVersion(), 0x100U);
	TS_ASSERT_EQUALS(page.getCreationTimestamp(), 0x02020202U);
	TS_ASSERT_EQUALS(page.getCreationTag(), 0x03030303U);
	TS_ASSERT_EQUALS(page.getPageSize(), PAGE_SIZE);
	TS_ASSERT_EQUALS(page.getHighestPageNumber(), 23U);
	TS_ASSERT_EQUALS(page.getTestHash(), 0x04030201U);
	TS_ASSERT_EQUALS(page.getHighestBucket(), 24U);
	TS_ASSERT_EQUALS(page.getDatabaseNumberOfRecords(), 42U);
	TS_ASSERT_EQUALS(page.getDataSize(), 0x0706050403020100ULL);
}

void HeaderPageTest::testEncodePage()
{
	static const size_type PAGE_SIZE = MIN_PAGE_SIZE;

	BucketHeaderPage page(allocator_.get(), PAGE_SIZE);
	page.clear();

	page.setMagic(0x078d75c8);
	page.setChecksum(0xe57bb835);
	page.setPageNumber(0);
	page.setDatabaseVersion(0x10abc);
	page.setCreationTimestamp(0x02020202);
	page.setCreationTag(0x03030303);
	page.setPageSize(PAGE_SIZE);
	page.setHighestPageNumber(24);
	page.setTestHash(0xE7F2C661);
	page.setHighestBucket(23);
	page.setDatabaseNumberOfRecords(42);
	page.setDataSize(0x0706050403020100LL);
	TS_ASSERT(page.dirty());

	uint8_t expectedPageData[PAGE_SIZE] = {
		0xc8, 0x75, 0x8d, 0x07, // 0: page type magic number - 0x078d75c8 = bucket file header
		0x35, 0xb8, 0x7b, 0xe5,	// 4: checksum -   0xe57bb835
		0, 0, 0, 0,				// 8: page number = 0
		0xbc, 0x0a, 1, 0,		// 12: database version = 0x10abc
		2, 2, 2, 2,				// 16: creation timestamp
		3, 3, 3, 3,				// 20: creation tag
		0, 4, 0, 0,				// 24: page size = 0x400 (1024)
		24, 0, 0, 0,			// 28: highest page number in this file = 24
		0x61, 0xc6, 0xf2, 0xe7,	// 32: value of hash(testKey) = 0x04030201
		23, 0, 0, 0,			// 36: highest bucket in use = 23
		42, 0, 0, 0, 0, 0, 0, 0,// 40: number of records in the database
		0, 1, 2, 3, 4, 5, 6, 7	// 48: size of key/value data in the database
	};

	TS_ASSERT_SAME_DATA(page.constData(), expectedPageData, PAGE_SIZE);

	page.setId(bucketFilePage(0));
	page.clearDirtyFlag();

	TS_ASSERT_EQUALS(0U, page.computeChecksum());
	TS_ASSERT_THROWS(page.validate(), IncompatibleDatabaseVersion);
	TS_ASSERT(! page.dirty());

	// Fix database version and checksum and try again.
	page.setDatabaseVersion(DATABASE_MINIMUM_FORMAT_VERSION);
	page.updateChecksum();
	TS_ASSERT_THROWS_NOTHING(page.validate());

	page.setDatabaseVersion(DATABASE_CURRENT_FORMAT_VERSION);
	page.updateChecksum();
	TS_ASSERT_THROWS_NOTHING(page.validate());

	page.clearDirtyFlag();
}

void HeaderPageTest::testSetUpAndValidateBucketPage()
{
	BucketHeaderPage bucketPage(allocator_.get(), MIN_PAGE_SIZE);
	
	bucketPage.setUp(0);
	TS_ASSERT(bucketPage.dirty());

	TS_ASSERT_THROWS(bucketPage.validate(), DatabaseCorruptedException);
	bucketPage.setChecksum(bucketPage.xor32());
	TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

	bucketPage.setHighestPageNumber(26);
	bucketPage.updateChecksum();
	bucketPage.clearDirtyFlag();

	TS_ASSERT_THROWS(bucketPage.validate(), DatabaseCorruptedException);
	TS_ASSERT(! bucketPage.dirty());
}

void HeaderPageTest::testSetUpAndValidateOverflowPage()
{
	OverflowHeaderPage overflowPage(allocator_.get(), MIN_PAGE_SIZE);

	overflowPage.setUp(0);
	TS_ASSERT(overflowPage.dirty());
	overflowPage.clearDirtyFlag();

	TS_ASSERT_THROWS(overflowPage.validate(), DatabaseCorruptedException);

	overflowPage.updateChecksum();
	overflowPage.clearDirtyFlag();
	TS_ASSERT_THROWS_NOTHING(overflowPage.validate());
}
