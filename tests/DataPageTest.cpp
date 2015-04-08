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
#include "utils/ConfigUtils.h"
#include "testUtils/TestPageAllocator.h"
#include "testUtils/StringUtils.h"
#include "db/BucketDataPage.h"
#include "db/OverflowDataPage.h"
#include "db/DataPageCursor.h"
#include "DataPageTest.h"

using namespace kerio::hashdb;

DataPageTest::DataPageTest()
	: allocator_(new TestPageAllocator())
{

}

//=============================================================================
// Test RecordId.

void DataPageTest::testInvalidRecordId()
{
	TS_ASSERT_THROWS(RecordId("", 1), InternalErrorException);
	TS_ASSERT_THROWS(RecordId(std::string(255, 'a'), 1), InternalErrorException);
	TS_ASSERT_THROWS(RecordId("a", -1), InternalErrorException);
	TS_ASSERT_THROWS(RecordId("a", 128), InternalErrorException);
}

void DataPageTest::testMinRecordId()
{
	// Minimalistic record id.
	const char expected1[] = {1, 'a', 0};
	const boost::string_ref key1("a");

	RecordId id1(key1, 0);
	boost::string_ref value1 = id1.value();

	TS_ASSERT_EQUALS(value1.size(), sizeof(expected1));
	TS_ASSERT_SAME_DATA(expected1, value1.data(), sizeof(expected1));
	TS_ASSERT_EQUALS(key1, id1.key());

	// Common record id.
	const char expected2[] = {2, 'a', 'z', 127};
	const boost::string_ref key2("az");

	RecordId id2(key2, 127);
	boost::string_ref value2 = id2.value();

	TS_ASSERT_EQUALS(sizeof(expected2), value2.size());
	TS_ASSERT_SAME_DATA(expected2, value2.data(), sizeof(expected2));
	TS_ASSERT_EQUALS(key2, id2.key());
}

void DataPageTest::testMaxRecordId()
{
	const std::string key(127, 'Z');

	std::string expected;
	expected += 127;
	expected += key;
	expected += 1;

	RecordId id(key, 1);
	boost::string_ref value = id.value();

	TS_ASSERT_EQUALS(expected.size(), value.size());
	TS_ASSERT_SAME_DATA(expected.data(), value.data(), static_cast<unsigned>(expected.size()));
	TS_ASSERT_EQUALS(key, id.key());
}

//=============================================================================
// Utilities for testing bucket data pages.

namespace {

	void initializeAndValidateBucketPage(BucketDataPage& bucketPage)
	{
		bucketPage.setUp(122345689);
		TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

		const size_type currentMaxValueSize = bucketPage.freeSpace() - sizeof(uint16_t); // free space - record pointer (2)
		const size_type largestPossibleValue = bucketPage.largestPossibleInlineRecordSize();
		TS_ASSERT_EQUALS(currentMaxValueSize, largestPossibleValue);
		TS_ASSERT_EQUALS(currentMaxValueSize, (bucketPage.size() - 22));

		uint8_t expectedEmptyPageHeader[] = {
			0x43, 0xd9, 0x19, 0x2e, // 0: page type magic number - 0x2e19d943 = bucket data page
			0xff, 0xff, 0xff, 0xff, // 4: checksum - 0xffffffff = not implemented
			0xd9, 0xd8, 0x4a, 0x07,	// 8: page number = 0x074ad8d9
			0, 0, 0, 0,				// 12: next overflow page number = 0
			0, 0,					// 16: number of records on the page = 0
			0, 0,					// 18: end of free area
			0, 0					// 20: key/value offset 0
		};

		// Fill end of free area.
		expectedEmptyPageHeader[18] = static_cast<uint8_t>(bucketPage.size() & 0xff);
		expectedEmptyPageHeader[19] = static_cast<uint8_t>(bucketPage.size() >> 8);

		TS_ASSERT_SAME_DATA(bucketPage.constData(), expectedEmptyPageHeader, sizeof(expectedEmptyPageHeader));
		TS_ASSERT(bucketPage.dirty());
	}

};

//=============================================================================
// Test BucketPage.

namespace {

	void doTestSetUpAndValidateBucketPage(IPageAllocator* allocator, size_type pageSize)
	{
		BucketDataPage bucketPage(allocator, pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		DataPageCursor cursor(&bucketPage);
		TS_ASSERT(! cursor.isValid());
		TS_ASSERT(! cursor.find(RecordId("x", 0)));

		bucketPage.clearDirtyFlag();
	}

};

void DataPageTest::testSetUpAndValidateBucketPage()
{
	TS_ASSERT_THROWS_NOTHING(doTestSetUpAndValidateBucketPage(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestSetUpAndValidateBucketPage(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}

//-----------------------------------------------------------------------------

namespace {

	void doTestAddEmptyRecord(IPageAllocator* allocator, size_type pageSize)
	{
		// Create and validate empty page.
		BucketDataPage bucketPage(allocator, pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		DataPageCursor cursor(&bucketPage);
		TS_ASSERT(! cursor.isValid());

		// Add a simple record to the page.
		const char recordKey[] = "a";
		const partNum_t recordPart = 127;

		RecordId recordId(recordKey, recordPart);
		DataPage::AddedValueRef valueRef("");
		const size_type addedSize = bucketPage.addSingleRecord(recordId, valueRef);
		TS_ASSERT_EQUALS(recordId.recordOverheadSize(), addedSize);
		TS_ASSERT(cursor.isValid());

		const size_type RECORD_SIZE = 5; // len (1) + key (1) + part number (1) + value size (2) + value (0)
		const size_type KEY_INDEX = pageSize - RECORD_SIZE;
		uint8_t expectedSinglePageHeader[] = {
			0x43, 0xd9, 0x19, 0x2e, // 0: page type magic number - 0x2e19d943 = bucket data page
			0xff, 0xff, 0xff, 0xff, // 4: checksum - 0xffffffff = not implemented
			0xd9, 0xd8, 0x4a, 0x07,	// 8: page number = 0x074ad8d9
			0, 0, 0, 0,				// 12: next overflow page number = 0
			1, 0,					// 16: number of records on the page = 1
			static_cast<uint8_t>(KEY_INDEX), static_cast<uint8_t>(KEY_INDEX >> 8), // 18: end of free area
			static_cast<uint8_t>(KEY_INDEX), static_cast<uint8_t>(KEY_INDEX >> 8)  // 20: key/value offset 0
		};

		TS_ASSERT_SAME_DATA(expectedSinglePageHeader, bucketPage.constData(), sizeof(expectedSinglePageHeader));

		uint8_t expectedSinglePageRecord[] = {
			1,			//	0:  key size = 1
			'a',		//	n:  key
			127,		//	n+1 part number (0..127)
			0, 0 		//	n+2 value size or 0xffff for big data
			//	n+4 value for small data
		};

		TS_ASSERT_SAME_DATA(expectedSinglePageRecord, bucketPage.constData() + KEY_INDEX, sizeof(expectedSinglePageRecord));

		// Cursor should be already on the item.
		TS_ASSERT(cursor.isValid());
		TS_ASSERT(cursor.isInlineValue());
		TS_ASSERT_EQUALS(3U, cursor.recordIdValue().size());
		TS_ASSERT_EQUALS(recordKey, cursor.key());
		TS_ASSERT_EQUALS(recordPart, cursor.partNum());
		TS_ASSERT_EQUALS(0U, cursor.inlineValue().size());

		// Find the record using the cursor.
		TS_ASSERT(cursor.find(recordId));
		TS_ASSERT(cursor.isValid());
		TS_ASSERT(cursor.isInlineValue());
		TS_ASSERT_EQUALS(3U, cursor.recordIdValue().size());
		TS_ASSERT_EQUALS(recordKey, cursor.key());
		TS_ASSERT_EQUALS(recordPart, cursor.partNum());
		TS_ASSERT_EQUALS(0U, cursor.inlineValue().size());

		// Next item should not be valid.
		cursor.next();
		TS_ASSERT(! cursor.isValid());

		bucketPage.clearDirtyFlag();
	}

}

void DataPageTest::testAddEmptyRecord()
{
	TS_ASSERT_THROWS_NOTHING(doTestAddEmptyRecord(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestAddEmptyRecord(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}

//-----------------------------------------------------------------------------

void DataPageTest::testAddMultipleSmallRecords()
{
	{
		static const size_type pageSize = MIN_PAGE_SIZE;

		BucketDataPage bucketPage(allocator_.get(), pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		int recordsAdded = 0;
		size_type endOfFreeArea = pageSize;

		for (;;) {
			const partNum_t recordNumber = recordsAdded + 1;

			TS_ASSERT_EQUALS(recordsAdded, bucketPage.getNumberOfRecords());
			TS_ASSERT_EQUALS(endOfFreeArea,bucketPage.getEndOfFreeArea());

			const std::string key = keyOfSize(recordNumber);
			const std::string value = valueOfSize(recordNumber);

			RecordId recordId(key, recordNumber);
			DataPage::AddedValueRef valueRef(value);
			const size_type addedSize = bucketPage.addSingleRecord(recordId, valueRef);

			if (addedSize == 0) {
				break;
			}

			TS_ASSERT_EQUALS(recordId.recordOverheadSize() + value.size(), addedSize);

			++recordsAdded;
			endOfFreeArea -= 4 + (2 * recordNumber); // len (1) + part num (1) + value size (2) + key + value

			DataPageCursor cursor(&bucketPage);
			TS_ASSERT(cursor.find(recordId));
			TS_ASSERT(cursor.isValid());
			TS_ASSERT_EQUALS(key, cursor.key());
			TS_ASSERT_EQUALS(recordNumber, cursor.partNum());
			TS_ASSERT(cursor.isInlineValue());
			TS_ASSERT_EQUALS(recordId.size(), cursor.recordIdValue().size());
			TS_ASSERT_SAME_DATA(recordId.data(), cursor.recordIdValue().data(), recordId.size());
		}

		TS_ASSERT_EQUALS(recordsAdded, bucketPage.getNumberOfRecords());

		uint8_t expectedMultiRecordPageHeader[] = {
			0x43, 0xd9, 0x19, 0x2e, // 0: page type magic number - 0x2e19d943 = bucket data page
			0xff, 0xff, 0xff, 0xff, // 4: checksum - 0xffffffff = not implemented
			0xd9, 0xd8, 0x4a, 0x07,	// 8: page number = 0x074ad8d9
			0, 0, 0, 0,				// 12: next overflow page number = 0
			28, 0					// 16: number of records on the page = 28
		};

		TS_ASSERT_SAME_DATA(expectedMultiRecordPageHeader, bucketPage.constData(), sizeof(expectedMultiRecordPageHeader));

		// Check that the page still holds all the data.
		for (partNum_t i = 0; i < recordsAdded; ++i) {
			const partNum_t recordNumber = i + 1;

			const std::string expectedKey = keyOfSize(recordNumber);
			const std::string expectedValue = valueOfSize(recordNumber);

			RecordId recordId(expectedKey, recordNumber);
			DataPageCursor cursor(&bucketPage);
			TS_ASSERT(cursor.find(recordId));
			TS_ASSERT(cursor.isValid());
			TS_ASSERT(cursor.isInlineValue());

			TS_ASSERT_EQUALS(recordId.size(), cursor.recordIdValue().size());
			TS_ASSERT_SAME_DATA(recordId.data(), cursor.recordIdValue().data(), recordId.size());

			TS_ASSERT(cursor.isInlineValue());
			TS_ASSERT_EQUALS(expectedValue.size(), cursor.inlineValue().size());
			TS_ASSERT_SAME_DATA(expectedValue.data(), cursor.inlineValue().data(), static_cast<unsigned>(expectedValue.size()));
		}

		bucketPage.clearDirtyFlag();
	}

	TS_ASSERT(allocator_->allFreed());
}

//=============================================================================
// Utilities for testing of record deletion.

namespace {

	bool addRecordOfValueSize(BucketDataPage& bucketPage, size_t n)
	{
		const std::string key = keyOfSize(n + 1);
		const std::string value = valueOfSize(n);

		RecordId recordId(key, static_cast<partNum_t>(n));
		DataPage::AddedValueRef valueRef(value);
		const size_type addedSize = bucketPage.addSingleRecord(recordId, valueRef);
		bool success = (addedSize != 0);

		if (success) {
			TS_ASSERT_EQUALS(recordId.recordOverheadSize() + value.size(), addedSize);
		}

		return success;
	}

	void deleteRecordOfValueSize(BucketDataPage& bucketPage, size_t n)
	{
		const std::string key = keyOfSize(n + 1);
		const std::string value = valueOfSize(n);

		RecordId recordId(key, static_cast<partNum_t>(n));
		DataPageCursor cursor(&bucketPage);

		TS_ASSERT(cursor.find(recordId));
		TS_ASSERT_EQUALS(key, cursor.key());
		TS_ASSERT_EQUALS(static_cast<partNum_t>(n), cursor.partNum());
		TS_ASSERT_EQUALS(value, cursor.inlineValue());

		bucketPage.deleteSingleRecord(cursor);

		cursor.reset();
		TS_ASSERT(! cursor.find(recordId));
	}

	void findAndValidateRecordOfValueSize(BucketDataPage& bucketPage, size_t n)
	{
		const std::string key = keyOfSize(n + 1);
		const std::string value = valueOfSize(n);

		RecordId recordId(key, static_cast<partNum_t>(n));
		DataPageCursor cursor(&bucketPage);

		TS_ASSERT(cursor.find(recordId));
		TS_ASSERT_EQUALS(key, cursor.key());
		TS_ASSERT_EQUALS(static_cast<partNum_t>(n), cursor.partNum());
		TS_ASSERT_EQUALS(value, cursor.inlineValue());
	}

};

//=============================================================================
// Testing deleting records from pages.

void DataPageTest::testDeleteLastRecords()
{
	{
		const size_type pageSize = defaultPageSize();
		BucketDataPage bucketPage(allocator_.get(), pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		// Create a single entry and delete it.
		const std::string firstKey = keyOfSize(1);
		static const partNum_t firstPartNum = 123;
		const std::string firstValue = valueOfSize(MIN_PAGE_SIZE);

		const RecordId firstRecordId(firstKey, firstPartNum);
		DataPage::AddedValueRef firstValueRef(firstValue);
		const size_type firstAddedSize = bucketPage.addSingleRecord(firstRecordId, firstValueRef);

		TS_ASSERT_EQUALS(firstRecordId.recordOverheadSize() + firstValue.size(), firstAddedSize);

		const DataPageCursor firstCursor(&bucketPage);
		TS_ASSERT_EQUALS(0, firstCursor.index());
		TS_ASSERT_EQUALS(firstKey, firstCursor.key());
		TS_ASSERT_EQUALS(firstPartNum, firstCursor.partNum());
		TS_ASSERT_EQUALS(firstValue, firstCursor.inlineValue());

		TS_ASSERT_THROWS_NOTHING(bucketPage.deleteSingleRecord(firstCursor));
		TS_ASSERT(! firstCursor.isValid());

		TS_ASSERT_EQUALS(bucketPage.size(), bucketPage.getEndOfFreeArea());
		TS_ASSERT_EQUALS(0, bucketPage.getNumberOfRecords());

		// Create multiple entries and delete them
		std::vector<uint32_t> endOfFreeAreaVector;

		unsigned recordNumber = 0;
		for (;; ++recordNumber) {
			if (! addRecordOfValueSize(bucketPage, recordNumber)) {
				break;
			}

			endOfFreeAreaVector.push_back(bucketPage.getEndOfFreeArea());
			TS_ASSERT_EQUALS(endOfFreeAreaVector.size(), bucketPage.getNumberOfRecords());
		}

		// We can fit in a small record and then delete it.
		const std::string lastKey("last");
		const partNum_t lastPartNum = 0;
		const RecordId lastRecordId(lastKey, lastPartNum);

		const size_type lastPossibleRecordSize = bucketPage.freeSpace() - sizeof(uint16_t); // free space - record pointer (2)
		const size_type lastRecordOverhead = lastRecordId.size() + sizeof(uint16_t);
		TS_ASSERT_EQUALS(lastRecordOverhead, lastRecordId.recordOverheadSize());
		TS_ASSERT(lastPossibleRecordSize > lastRecordOverhead);

		const size_type lastValueSize = lastPossibleRecordSize - lastRecordOverhead;
		const std::string lastValue = valueOfSize(lastValueSize);
		TS_ASSERT_EQUALS(lastPossibleRecordSize, lastRecordOverhead + lastValueSize);

		DataPage::AddedValueRef lastValueRef(lastValue);
		const size_type lastAddedSize = bucketPage.addSingleRecord(lastRecordId, lastValueRef);

		TS_ASSERT_EQUALS(lastRecordId.recordOverheadSize() + lastValue.size(), lastAddedSize);
		TS_ASSERT_EQUALS(0U, bucketPage.freeSpace());

		DataPageCursor lastCursor(&bucketPage);
		TS_ASSERT(lastCursor.find(lastRecordId));
		TS_ASSERT_EQUALS(lastRecordOverhead, lastCursor.recordOverheadSize());
		TS_ASSERT_EQUALS(lastKey, lastCursor.key());
		TS_ASSERT_EQUALS(lastPartNum, lastCursor.partNum());
		TS_ASSERT_EQUALS(lastValue, lastCursor.inlineValue());

		TS_ASSERT_THROWS_NOTHING(bucketPage.deleteSingleRecord(lastCursor));
		TS_ASSERT(! lastCursor.isValid());

		while (recordNumber--) {
			TS_ASSERT_EQUALS(endOfFreeAreaVector.size(), bucketPage.getNumberOfRecords());
			TS_ASSERT(! endOfFreeAreaVector.empty());
			const uint32_t expectedEndOfFreeArea = endOfFreeAreaVector.back();
			const uint32_t actualEndOfFreeArea = bucketPage.getEndOfFreeArea();
			TS_ASSERT_EQUALS(expectedEndOfFreeArea, actualEndOfFreeArea);
			endOfFreeAreaVector.pop_back();

			TS_ASSERT_THROWS_NOTHING(deleteRecordOfValueSize(bucketPage, recordNumber));
		}

		TS_ASSERT(endOfFreeAreaVector.empty());

		bucketPage.clearDirtyFlag();
	}

	TS_ASSERT(allocator_->allFreed());
}

//-----------------------------------------------------------------------------

void DataPageTest::testDeleteFirstRecords()
{
	{
		const size_type pageSize = defaultPageSize();
		BucketDataPage bucketPage(allocator_.get(), pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		// Create records.
		unsigned maxRecordNumber = 0;
		for (unsigned recordNumber = 0;; ++recordNumber) {
			if (! addRecordOfValueSize(bucketPage, recordNumber)) {
				break;
			}

			maxRecordNumber = recordNumber;
		}

		// Delete them in order.
		for (unsigned recordNumber = 0; recordNumber <= maxRecordNumber; ++recordNumber) {
			TS_ASSERT_THROWS_NOTHING(deleteRecordOfValueSize(bucketPage, recordNumber));

			// Can we still read remaining records?
			for (unsigned j = recordNumber + 1; j <= maxRecordNumber; ++ j) {
				TS_ASSERT_THROWS_NOTHING(findAndValidateRecordOfValueSize(bucketPage, j));
			}
		}

		bucketPage.clearDirtyFlag();
	}

	TS_ASSERT(allocator_->allFreed());
}

//-----------------------------------------------------------------------------

void DataPageTest::testDeleteMiddleRecords()
{
	{
		const size_type pageSize = defaultPageSize();
		BucketDataPage bucketPage(allocator_.get(), pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		// Create records.
		unsigned maxRecordNumber = 0;
		for (unsigned recordNumber = 0;; ++recordNumber) {
			if (! addRecordOfValueSize(bucketPage, recordNumber)) {
				break;
			}

			maxRecordNumber = recordNumber;
		}

		// Leave first 3 records and delete the remaining ones.
		for (unsigned recordNumber = 3; recordNumber <= maxRecordNumber; ++recordNumber) {
			TS_ASSERT_THROWS_NOTHING(deleteRecordOfValueSize(bucketPage, recordNumber));
			TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

			// Can we still read remaining records?
			for (unsigned j = 0; j < 3; ++ j) {
				TS_ASSERT_THROWS_NOTHING(findAndValidateRecordOfValueSize(bucketPage, j));		
			}

			for (unsigned j = recordNumber + 1; j <= maxRecordNumber; ++ j) {
				TS_ASSERT_THROWS_NOTHING(findAndValidateRecordOfValueSize(bucketPage, j));
			}
		}

		// Delete second record.
		TS_ASSERT_THROWS_NOTHING(deleteRecordOfValueSize(bucketPage, 1));
		TS_ASSERT_THROWS_NOTHING(findAndValidateRecordOfValueSize(bucketPage, 0));
		TS_ASSERT_THROWS_NOTHING(findAndValidateRecordOfValueSize(bucketPage, 2));
		TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

		// Delete first record.
		TS_ASSERT_THROWS_NOTHING(deleteRecordOfValueSize(bucketPage, 0));
		TS_ASSERT_THROWS_NOTHING(findAndValidateRecordOfValueSize(bucketPage, 2));
		TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

		// Delete last record.
		TS_ASSERT_THROWS_NOTHING(deleteRecordOfValueSize(bucketPage, 2));

		TS_ASSERT_EQUALS(0, bucketPage.getNumberOfRecords());
		TS_ASSERT_EQUALS(bucketPage.size(), bucketPage.getEndOfFreeArea());
		TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

		bucketPage.clearDirtyFlag();
	}

	TS_ASSERT(allocator_->allFreed());
}

//-----------------------------------------------------------------------------

namespace {

	void doTestDeleteSingleLargeItem(IPageAllocator* allocator, size_type pageSize)
	{
		BucketDataPage bucketPage(allocator, pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		const size_type largestValue = bucketPage.largestPossibleInlineRecordSize();
		RecordId recordId("a", 22);
		const size_type valueSize = largestValue - recordId.recordOverheadSize();

		const std::string value = valueOfSize(valueSize);
		const std::string oversizedValue = value + " ";

		const DataPage::AddedValueRef oversizedValueRef(oversizedValue);
		const size_type addOversizedResult = bucketPage.addSingleRecord(recordId, oversizedValueRef);
		TS_ASSERT_EQUALS(0U, addOversizedResult);


		const DataPage::AddedValueRef valueRef(value);
		const size_type addResult = bucketPage.addSingleRecord(recordId, valueRef);
		TS_ASSERT_EQUALS(recordId.recordOverheadSize() + value.size(), addResult);
		TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

		DataPageCursor cursor(&bucketPage);

		TS_ASSERT(cursor.find(recordId));
		TS_ASSERT_EQUALS("a", cursor.key());
		TS_ASSERT_EQUALS(22, cursor.partNum());
		TS_ASSERT_EQUALS(value, cursor.inlineValue());

		bucketPage.deleteSingleRecord(cursor);

		cursor.reset();
		TS_ASSERT(! cursor.find(recordId));
		TS_ASSERT_EQUALS(0, bucketPage.getNumberOfRecords());
		TS_ASSERT_EQUALS(bucketPage.size(), bucketPage.getEndOfFreeArea());
		TS_ASSERT_THROWS_NOTHING(bucketPage.validate());

		bucketPage.clearDirtyFlag();
	}

};

void DataPageTest::testDeleteSingleLargeItem()
{
	TS_ASSERT_THROWS_NOTHING(doTestDeleteSingleLargeItem(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestDeleteSingleLargeItem(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}

//=============================================================================
// Test OverflowPage.

namespace {

	void doTestOverflowPage(IPageAllocator* allocator, size_type pageSize)
	{
		OverflowDataPage overflowPage(allocator, pageSize);

		overflowPage.setUp(122345689);
		TS_ASSERT_THROWS_NOTHING(overflowPage.validate());

		const size_type currentMaxValueSize = overflowPage.freeSpace() - sizeof(uint16_t); // free space - record pointer (2)
		const size_type largestPossibleValue = overflowPage.largestPossibleInlineRecordSize();
		TS_ASSERT_EQUALS(currentMaxValueSize, largestPossibleValue);
		TS_ASSERT_EQUALS(currentMaxValueSize, (overflowPage.size() - 22));

		uint8_t expectedEmptyPageHeader[] = {
			0x97, 0xf2, 0xe2, 0x35, // 0: page type magic number - 0x35e2f297 = overflow data page
			0xff, 0xff, 0xff, 0xff, // 4: checksum - 0xffffffff = not implemented
			0xd9, 0xd8, 0x4a, 0x07,	// 8: page number = 0x074ad8d9
			0, 0, 0, 0,				// 12: next overflow page number = 0
			0, 0,					// 16: number of records on the page = 0
			static_cast<uint8_t>(pageSize), static_cast<uint8_t>(pageSize >> 8),	// 18: end of free area
			0, 0					// 20: key/value offset 0
		};

		TS_ASSERT_SAME_DATA(overflowPage.constData(), expectedEmptyPageHeader, sizeof(expectedEmptyPageHeader));

		// Create a single entry and delete it.
		const std::string key = keyOfSize(1);
		const partNum_t partNum = 123;
		const std::string value = valueOfSize(MIN_PAGE_SIZE / 2);

		const RecordId recordId(key, partNum);
		const DataPage::AddedValueRef valueRef(value);

		const size_type addResult = overflowPage.addSingleRecord(recordId, valueRef);
		TS_ASSERT_EQUALS(recordId.recordOverheadSize() + value.size(), addResult);

		const DataPageCursor cursor(&overflowPage);
		TS_ASSERT_EQUALS(0, cursor.index());
		TS_ASSERT_EQUALS(key, cursor.key());
		TS_ASSERT_EQUALS(partNum, cursor.partNum());
		TS_ASSERT_EQUALS(value, cursor.inlineValue());

		TS_ASSERT_THROWS_NOTHING(overflowPage.deleteSingleRecord(cursor));
		TS_ASSERT(! cursor.isValid());

		TS_ASSERT_EQUALS(overflowPage.size(), overflowPage.getEndOfFreeArea());
		TS_ASSERT_EQUALS(0, overflowPage.getNumberOfRecords());

		overflowPage.clearDirtyFlag();
	}

};

void DataPageTest::testOverflowPage()
{
	TS_ASSERT_THROWS_NOTHING(doTestOverflowPage(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestOverflowPage(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}

//=============================================================================
// Accessing whole inline part of the record.

namespace {

	void doTestCursorGetInlineRecordData(IPageAllocator* allocator, size_type pageSize)
	{
		BucketDataPage bucketPage(allocator, pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(bucketPage));

		const std::string key = keyOfSize(1);

		// Create inline entry.
		const std::string firstValue = valueOfSize(pageSize / 2);
		const RecordId firstRecordId(key, 0);
		DataPage::AddedValueRef firstValueRef(firstValue);
		const size_type firstAddedSize = bucketPage.addSingleRecord(firstRecordId, firstValueRef);
		TS_ASSERT(firstAddedSize != 0);

		// Create large value entry.
		const RecordId secondRecordId(key, 1);
		const size_type secondValueSize = pageSize + 13;
		const PageId& secondValuePage = overflowFilePage(0x2c49e6a3);
		DataPage::AddedValueRef secondValueRef(secondValueSize, secondValuePage);
		const size_type secondAddedSize = bucketPage.addSingleRecord(secondRecordId, secondValueRef);
		TS_ASSERT(secondAddedSize != 0);

		// Validate cursor readings.
		DataPageCursor cursor(&bucketPage);
		TS_ASSERT(cursor.isValid());
		TS_ASSERT(cursor.isInlineValue());
		TS_ASSERT_EQUALS(key, cursor.key());
		TS_ASSERT_EQUALS(0, cursor.partNum());
		TS_ASSERT_EQUALS(firstValue, cursor.inlineValue());

		const boost::string_ref firstRecord = cursor.inlineRecord();
		TS_ASSERT_EQUALS(cursor.recordIdValue().data(), firstRecord.data());
		TS_ASSERT_EQUALS(firstAddedSize, firstRecord.size());

		cursor.next();
		TS_ASSERT(cursor.isValid());
		TS_ASSERT(! cursor.isInlineValue());
		TS_ASSERT_EQUALS(key, cursor.key());
		TS_ASSERT_EQUALS(1, cursor.partNum());
		TS_ASSERT_EQUALS(secondValueSize, cursor.largeValueSize());
		TS_ASSERT_EQUALS(secondValuePage, cursor.firstLargeValuePageId());

		const boost::string_ref secondRecord = cursor.inlineRecord();
		TS_ASSERT_EQUALS(cursor.recordIdValue().data(), secondRecord.data());
		TS_ASSERT_EQUALS(secondAddedSize, secondRecord.size());

		cursor.next();
		TS_ASSERT(! cursor.isValid());

		bucketPage.clearDirtyFlag();
	}

};

void DataPageTest::testCursorGetInlineRecordData()
{
	TS_ASSERT_THROWS_NOTHING(doTestCursorGetInlineRecordData(allocator_.get(), MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestCursorGetInlineRecordData(allocator_.get(), MAX_PAGE_SIZE));
	TS_ASSERT(allocator_->allFreed());
}

//=============================================================================
// Adding whole inline part of the record.

void DataPageTest::testAddInlineRecordData()
{
	{
		const size_type pageSize = MIN_PAGE_SIZE;

		BucketDataPage originalBucketPage(allocator_.get(), pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(originalBucketPage));

		BucketDataPage copiedBucketPage(allocator_.get(), pageSize);
		TS_ASSERT_THROWS_NOTHING(initializeAndValidateBucketPage(copiedBucketPage));

		const std::string key = keyOfSize(1);
		const size_type secondRecordInlineSize = 13; // record id (3) + tag (2) + large value reference (8) = 13
		const size_type firstRecordInlineSize = originalBucketPage.largestPossibleInlineRecordSize() - secondRecordInlineSize - sizeof(uint16_t);

		// Create inline entry.
		const std::string firstValue = valueOfSize(firstRecordInlineSize - 5);
		const RecordId firstRecordId(key, 0);
		DataPage::AddedValueRef firstValueRef(firstValue);
		const size_type firstAddedSize = originalBucketPage.addSingleRecord(firstRecordId, firstValueRef);
		TS_ASSERT_EQUALS(firstRecordInlineSize, firstAddedSize);

		// Create large value entry.
		const RecordId secondRecordId(key, 1);
		const size_type secondValueSize = pageSize + 29;
		const PageId& secondValuePage = overflowFilePage(0xa7c4ac65);
		DataPage::AddedValueRef secondValueRef(secondValueSize, secondValuePage);
		const size_type secondAddedSize = originalBucketPage.addSingleRecord(secondRecordId, secondValueRef);
		TS_ASSERT_EQUALS(secondRecordInlineSize, secondAddedSize);
		TS_ASSERT_EQUALS(0U, originalBucketPage.freeSpace());

		// Validate cursor readings and copy to second page.
		DataPageCursor cursor(&originalBucketPage);
		TS_ASSERT(cursor.isValid());
		TS_ASSERT(cursor.isInlineValue());
		TS_ASSERT_EQUALS(key, cursor.key());
		TS_ASSERT_EQUALS(0, cursor.partNum());
		TS_ASSERT_EQUALS(firstValue, cursor.inlineValue());

		const boost::string_ref firstRecord = cursor.inlineRecord();
		TS_ASSERT_EQUALS(cursor.recordIdValue().data(), firstRecord.data());
		TS_ASSERT_EQUALS(firstAddedSize, firstRecord.size());

		const size_type firstCopyAddedSize = copiedBucketPage.addSingleRecord(firstRecord);
		TS_ASSERT_EQUALS(firstRecordInlineSize, firstCopyAddedSize);

		cursor.next();
		TS_ASSERT(cursor.isValid());
		TS_ASSERT(! cursor.isInlineValue());
		TS_ASSERT_EQUALS(key, cursor.key());
		TS_ASSERT_EQUALS(1, cursor.partNum());
		TS_ASSERT_EQUALS(secondValueSize, cursor.largeValueSize());
		TS_ASSERT_EQUALS(secondValuePage, cursor.firstLargeValuePageId());

		const boost::string_ref secondRecord = cursor.inlineRecord();
		TS_ASSERT_EQUALS(cursor.recordIdValue().data(), secondRecord.data());
		TS_ASSERT_EQUALS(secondAddedSize, secondRecord.size());

		const size_type secondCopyAddedSize = copiedBucketPage.addSingleRecord(secondRecord);
		TS_ASSERT_EQUALS(secondRecordInlineSize, secondCopyAddedSize);
		TS_ASSERT_EQUALS(0U, copiedBucketPage.freeSpace());

		cursor.next();
		TS_ASSERT(! cursor.isValid());

		// Validate cursor readings in the copied page.
		DataPageCursor copiedCursor(&copiedBucketPage);
		TS_ASSERT(copiedCursor.isValid());
		TS_ASSERT(copiedCursor.isInlineValue());
		TS_ASSERT_EQUALS(key, copiedCursor.key());
		TS_ASSERT_EQUALS(0, copiedCursor.partNum());
		TS_ASSERT_EQUALS(firstValue, copiedCursor.inlineValue());
		TS_ASSERT_EQUALS(firstRecord, copiedCursor.inlineRecord());

		copiedCursor.next();
		TS_ASSERT(copiedCursor.isValid());
		TS_ASSERT(! copiedCursor.isInlineValue());
		TS_ASSERT_EQUALS(key, copiedCursor.key());
		TS_ASSERT_EQUALS(1, copiedCursor.partNum());
		TS_ASSERT_EQUALS(secondValueSize, copiedCursor.largeValueSize());
		TS_ASSERT_EQUALS(secondValuePage, copiedCursor.firstLargeValuePageId());
		TS_ASSERT_EQUALS(secondRecord, copiedCursor.inlineRecord());

		copiedCursor.next();
		TS_ASSERT(! copiedCursor.isValid());

		originalBucketPage.clearDirtyFlag();
		copiedBucketPage.clearDirtyFlag();
	}

	TS_ASSERT(allocator_->allFreed());
}
