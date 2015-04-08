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
#include <kerio/hashdb/Exception.h>
#include "utils/ConfigUtils.h"
#include "db/PagedFile.h"
#include "db/BucketDataPage.h"
#include "testUtils/FileUtils.h"
#include "testUtils/TestPageAllocator.h"
#include "PagedFileTest.h"

using namespace kerio::hashdb;

//-----------------------------------------------------------------------------
// Fixtures.

PagedFileTest::PagedFileTest()
	: fileName_(getTestPath() + "/db")
	, environment_(Options::readWriteSingleThreaded())
	, allocator_(new TestPageAllocator())
{

}

void PagedFileTest::setUp()
{
	removeTestDirectory();
	createTestDirectory();
}

void PagedFileTest::tearDown()
{
	removeTestDirectory();
}

//-----------------------------------------------------------------------------
// Tests

void PagedFileTest::testFileOpen()
{
	// Attempt to open file with empty filename should fail.
	TS_ASSERT_THROWS(PagedFile("", PageId::BucketFileType, Options::readWriteSingleThreaded(), environment_), InternalErrorException);

	// Attempt to open file with invalid file type should fail.
	TS_ASSERT_THROWS(PagedFile(fileName_, PageId::InvalidFileType, Options::readWriteSingleThreaded(), environment_), InternalErrorException);
	
	// Attempt to open non-existing file should fail.
	Options options = Options::readWriteSingleThreaded();
	options.createIfMissing_ = false;
	TS_ASSERT_THROWS(PagedFile(fileName_, PageId::BucketFileType, options, environment_), IoException);

	// Create non-existing file should succeed.
	options.createIfMissing_ = true;
	boost::scoped_ptr<PagedFile> file;

	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));
	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(file.reset());

	// Open of existing file should succeed.
	options.createIfMissing_ = false;
	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));
	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(file.reset());

	// Attempt to open existing file with invalid options should fail.
	options.pageSize_ = 13;
	TS_ASSERT_THROWS(PagedFile(fileName_, PageId::BucketFileType, options, environment_), InvalidArgumentException);

	// Cleanup.
	TS_ASSERT_THROWS_NOTHING(deleteFile(fileName_));
}

void PagedFileTest::testPageSize()
{
	static const size_type initialPageSize = defaultPageSize();
	static const size_type biggerPageSize = 2 * defaultPageSize();

	// Create paged file and check page size.
	Options options = Options::readWriteSingleThreaded();
	boost::scoped_ptr<PagedFile> file;

	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));
	TS_ASSERT_EQUALS(file->pageSize(), options.pageSize_);
	TS_ASSERT_EQUALS(file->pageSize(), initialPageSize);

	// Page size cannot be set to invalid values.
	TS_ASSERT_THROWS(file->setPageSize(initialPageSize + 1), InternalErrorException);
	TS_ASSERT_EQUALS(file->pageSize(), initialPageSize);

	// Set new page size and check it.
	file->setPageSize(biggerPageSize);
	TS_ASSERT_EQUALS(file->pageSize(), biggerPageSize);

	// Check that page size is checked by read and write methods.
	BucketDataPage page(allocator_.get(), initialPageSize);
	page.clear();

	page.setId(bucketFilePage(0));
	TS_ASSERT_THROWS(file->write(page), InternalErrorException);

	TS_ASSERT_THROWS(file->read(page, bucketFilePage(0)), InternalErrorException);

	// Cleanup.
	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(deleteFile(fileName_));
	page.clearDirtyFlag();
}

void PagedFileTest::testReadWrite()
{
	static const size_type PAGE_SIZE = 1024;
	static const size_type TEST_PAGES = 13;

	// Create paged file.
	Options options = Options::readWriteSingleThreaded();
	options.pageSize_ = PAGE_SIZE;

	boost::scoped_ptr<PagedFile> file;
	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));
	TS_ASSERT_EQUALS(file->size(), 0U);

	// Write 13 pages.
	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	for (size_type i = 0; i < TEST_PAGES; ++i) {
		page.setId(bucketFilePage(i));

		page.clear();
		page[0] = static_cast<Page::value_type>(i);
		page[PAGE_SIZE - 1] = static_cast<Page::value_type>(i + 31);
		TS_ASSERT(page.dirty());
		TS_ASSERT_THROWS_NOTHING(file->write(page));
		TS_ASSERT_EQUALS(file->size(), (i + 1) * PAGE_SIZE);
	}

	// A non-dirty page is not written.
	TS_ASSERT_EQUALS(file->size(), TEST_PAGES * PAGE_SIZE);
	TS_ASSERT(! page.dirty());
	page.setId(bucketFilePage(TEST_PAGES));
	TS_ASSERT_THROWS_NOTHING(file->write(page));
	TS_ASSERT_EQUALS(file->size(), TEST_PAGES * PAGE_SIZE);

	// Read and check 13 pages.
	for (size_type i = 0; i < TEST_PAGES; ++i) {
		TS_ASSERT_THROWS_NOTHING(file->read(page, bucketFilePage(i)));

		TS_ASSERT_EQUALS(page.get8(0), i);
		TS_ASSERT_EQUALS(page.get8(1), 0);
		TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 2), 0);
		TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 1), i + 31);
	}

	// Close and open file.
	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(file.reset());
	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));

	// Read and check 13 pages.
	for (size_type i = 0; i < TEST_PAGES; ++i) {
		TS_ASSERT_THROWS_NOTHING(file->read(page, bucketFilePage(i)));

		TS_ASSERT_EQUALS(page.get8(0), i);
		TS_ASSERT_EQUALS(page.get8(1), 0);
		TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 2), 0);
		TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 1), i + 31);
	}

	// Check that read beyond end-of-file fails.
	TS_ASSERT_THROWS(file->read(page, PageId(PageId::BucketFileType, TEST_PAGES)), IoException);

	// Overwrite a single page, and check that the change is persistent.
	const PageId changedPageId = bucketFilePage(3);
	page.setId(changedPageId);
	page.clear();
	page[0] = 98;
	page[PAGE_SIZE - 1] = 44;
	TS_ASSERT(page.dirty());
	TS_ASSERT_THROWS_NOTHING(file->write(page));
	TS_ASSERT_EQUALS(file->size(), TEST_PAGES * PAGE_SIZE);

	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(file.reset());
	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));

	for (size_type i = 0; i < TEST_PAGES; ++i) {
		TS_ASSERT_THROWS_NOTHING(file->read(page, bucketFilePage(i)));

		if (page.getId() == changedPageId) {
			TS_ASSERT_EQUALS(page.get8(0), 98);
			TS_ASSERT_EQUALS(page.get8(1), 0);
			TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 2), 0);
			TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 1), 44);
		}
		else {
			TS_ASSERT_EQUALS(page.get8(0), i);
			TS_ASSERT_EQUALS(page.get8(1), 0);
			TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 2), 0);
			TS_ASSERT_EQUALS(page.get8(PAGE_SIZE - 1), i + 31);
		}
	}

	// Cleanup.
	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(file.reset());
	TS_ASSERT_THROWS_NOTHING(deleteFile(fileName_));
}

void PagedFileTest::testReadOnly()
{
	static const unsigned TEST_PAGES = 3;

	// Create a paged file.
	Options options = Options::readWriteSingleThreaded();

	boost::scoped_ptr<PagedFile> file;
	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));
	TS_ASSERT_EQUALS(file->size(), 0U);

	// Write 3 pages.
	BucketDataPage page(allocator_.get(), defaultPageSize());
	for (unsigned i = 0; i < TEST_PAGES; ++i) {
		page.clear();
		page.setId(bucketFilePage(i));
		TS_ASSERT_THROWS_NOTHING(file->write(page));
	}
	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(file.reset());

	// Open R/O
	options.createIfMissing_ = false;
	options.readOnly_ = true;
	TS_ASSERT_THROWS_NOTHING(file.reset(new PagedFile(fileName_, PageId::BucketFileType, options, environment_)));
	TS_ASSERT_EQUALS(file->size(), TEST_PAGES * defaultPageSize());

	// Attempt to write to it.
	page.setId(bucketFilePage(1));
	page.clear(); // Marks it dirty.
	TS_ASSERT_THROWS(file->write(page), IoException);

	// Cleanup.
	TS_ASSERT_THROWS_NOTHING(file->close());
	TS_ASSERT_THROWS_NOTHING(file.reset());
	TS_ASSERT_THROWS_NOTHING(deleteFile(fileName_));
	page.clearDirtyFlag();
}
