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
#include <kerio/hashdb/Constants.h>
#include <kerio/hashdbHelpers/StringWriteBatch.h>
#include <kerio/hashdbHelpers/StringReadBatch.h>
#include <kerio/hashdbHelpers/DeleteBatch.h>
#include "utils/ConfigUtils.h"
#include "testUtils/FileUtils.h"
#include "testUtils/StringUtils.h"
#include "testUtils/TestPageAllocator.h"
#include "testUtils/TestLogger.h"
#include "testUtils/PrintException.h"
#include "testUtils/StringCopyWriteBatch.h"
#include "testUtils/StringCopyReadBatch.h"
#include "testUtils/StringCopyDeleteBatch.h"
#include "DatabaseTest.h"


using namespace kerio::hashdb;

DatabaseTest::DatabaseTest()
	: databaseTestPath_(getTestPath())
	, allocator_(new TestPageAllocator())
{
	
}

void DatabaseTest::setUp()
{
	removeTestDirectory();
	createTestDirectory();
}

void DatabaseTest::tearDown()
{
	removeTestDirectory();
}

//-----------------------------------------------------------------------------

void DatabaseTest::testOpenInvalid()
{
	const std::string name = databaseTestPath_ + "/testdb";

	Database db = DatabaseFactory();

	// Open with empty database name.
	{	
		TS_ASSERT_THROWS(db->open("", Options::readWriteSingleThreaded()), InvalidArgumentException);
	}

	// Invalid options: cannot create and open R/O at the same time
	{	
		Options options = Options::readWriteSingleThreaded();
		options.createIfMissing_ = true;
		options.readOnly_ = true;
		options.logger_.reset(new TestLogger());
		TS_ASSERT_THROWS(db->open(name, options), InvalidArgumentException);
	}

	// Invalid options: invalid page size
	{	
		Options options = Options::readWriteSingleThreaded();
		options.pageSize_ = 3;
		TS_ASSERT_THROWS(db->open(name, options), InvalidArgumentException);
	}

	// Invalid options: 0 bucket pages
	{	
		Options options = Options::readWriteSingleThreaded();
		options.initialBuckets_ = 0;
		TS_ASSERT_THROWS(db->open(name, options), InvalidArgumentException);
	}

	// Invalid options: SingleThreadedPageAllocatorType cannot be used with TrueLockManagerType
	{	
		Options options = Options::readWriteSingleThreaded();
		options.lockManagerType_ = Options::TrueLockManagerType;
		options.pageAllocatorType_ = Options::SingleThreadedPageAllocatorType;
		TS_ASSERT_THROWS(db->open(name, options), InvalidArgumentException);
		TS_ASSERT_THROWS(db->statistics(), InvalidArgumentException);
	}
}

//-----------------------------------------------------------------------------

namespace {

	void doTestCreateEmpty(Database db, const std::string& name, size_type pageSize)
	{
		// Create, close, open.
		boost::shared_ptr<TestLogger> testLogger(new TestLogger);

		Options options = Options::readWriteSingleThreaded();
		options.pageSize_ = pageSize;
		options.logger_ = testLogger;

		TS_ASSERT_THROWS_NOTHING(db->open(name, options));
		Statistics stats = db->statistics();

		TS_ASSERT_EQUALS(1U, stats.bucketPagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
		TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
		TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
		TS_ASSERT_EQUALS(0U, stats.overflowFileDataPages_);
		TS_ASSERT_EQUALS(0U, stats.overflowFileBitmapPages_);
		TS_ASSERT_EQUALS(0U, stats.numberOfRecords_);
		TS_ASSERT_EQUALS(0U, stats.dataInlineSize_);

		TS_ASSERT_THROWS_NOTHING(db->sync());
		TS_ASSERT_THROWS_NOTHING(db->close());

		options.createIfMissing_ = false;
		TS_ASSERT_THROWS_NOTHING(db->open(name, options));

		stats = db->statistics();
		TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
		TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
		TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
		TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
		TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
		TS_ASSERT_EQUALS(0U, stats.overflowFileDataPages_);
		TS_ASSERT_EQUALS(0U, stats.overflowFileBitmapPages_);
		TS_ASSERT_EQUALS(0U, stats.numberOfRecords_);
		TS_ASSERT_EQUALS(0U, stats.dataInlineSize_);

		TS_ASSERT_THROWS_NOTHING(db->close());

		TS_ASSERT(testLogger->messages() != 0);
	}

};

void DatabaseTest::testCreateEmpty()
{
	Database db = DatabaseFactory();

	TS_ASSERT_THROWS_NOTHING(doTestCreateEmpty(db, databaseTestPath_ + "/minpages", MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestCreateEmpty(db, databaseTestPath_ + "/maxpages", MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

void DatabaseTest::testOpenCorrupted()
{
	const std::string name = databaseTestPath_ + "/corrupted";

	Database db = DatabaseFactory();
	
	// Create database.
	{
		TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));
		TS_ASSERT_THROWS_NOTHING(db->close());
	}

	// Corrupt bucket header - overwrite checksum.
	Options options = Options::readWriteSingleThreaded();
	corruptFile32(name + ".dbb", options.pageSize_, 0, 4, 1234); // checksum
	TS_ASSERT_THROWS(db->open(name, options), DatabaseCorruptedException);
}

void DatabaseTest::testOpenUnsupportedVersion()
{
	Options options = Options::readWriteSingleThreaded();
	const std::string name = databaseTestPath_ + "/oldVersion";

	Database db = DatabaseFactory();

	// Create database.
	{
		TS_ASSERT_THROWS_NOTHING(db->open(name, options));
		TS_ASSERT_THROWS_NOTHING(db->close());
	}

	// Corrupt bucket header - overwrite database version and checksum.
	const std::string fileName = name + ".dbb";
	corruptFile32(fileName, options.pageSize_, 0, 12, 0);			// format version of the current database
	corruptFile32(fileName, options.pageSize_, 0, 4, 0xffffffff);	// checksum
	TS_ASSERT_THROWS(db->open(name, options), IncompatibleDatabaseVersion);
}

void DatabaseTest::testInvalidArguments()
{
	const std::string name = databaseTestPath_ + "/db";

	Database db = DatabaseFactory();
	TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

	TS_ASSERT_THROWS(db->store("", 0, ""), InvalidArgumentException);
	TS_ASSERT_THROWS(db->store("a", -1, ""), InvalidArgumentException);
	TS_ASSERT_THROWS(db->store("a", MAX_PARTNUM+1, ""), InvalidArgumentException);

	std::string readBytes;
	TS_ASSERT_THROWS(db->fetch("", 0, readBytes), InvalidArgumentException);
	TS_ASSERT_THROWS(db->fetch("a", -1, readBytes), InvalidArgumentException);
	TS_ASSERT_THROWS(db->fetch("a", MAX_PARTNUM+1, readBytes), InvalidArgumentException);

	TS_ASSERT_THROWS(db->remove(""),  InvalidArgumentException);
	TS_ASSERT_THROWS(db->remove("", 0),  InvalidArgumentException);
	TS_ASSERT_THROWS(db->remove("a", -1),  InvalidArgumentException);
	TS_ASSERT_THROWS(db->remove("a", MAX_PARTNUM+1),  InvalidArgumentException);
}

namespace {

	//-------------------------------------------------------------------------

	class RecordsIteratedOver {
	public:
		RecordsIteratedOver(Database db)
		{
			for (Iterator iterator = db->newIterator(); iterator->isValid(); iterator->next()) {
				Record record;

				record.key_ = iterator->key();
				record.partNum_ = iterator->partNum();
				record.value_ = iterator->value();

				TS_ASSERT_EQUALS(record.value_.size(), iterator->valueSize());

				contents_.push_back(record);
			}
		}

		size_t size() const
		{
			return contents_.size();
		}

		bool empty() const
		{
			return contents_.empty();
		}

		bool checkAndRemove(const std::string& key, partNum_t partNum, const std::string& value)
		{
			bool success = false;

			for (std::vector<Record>::iterator ii = contents_.begin(); ii != contents_.end(); ++ii) {
				if (ii->key_ == key && ii->partNum_ == partNum) {
					TS_ASSERT_EQUALS(ii->value_, value);
					contents_.erase(ii);
					success = true;
					break;
				}
			}

			return success;
		}

	private:
		struct Record {
			std::string key_;
			partNum_t partNum_;
			std::string value_;
		};

		std::vector<Record> contents_;
	};

	//-------------------------------------------------------------------------

	void checkDatabaseIsEmpty(Database db)
	{
		RecordsIteratedOver records(db);
		TS_ASSERT(records.empty());

		Statistics stats = db->statistics();
		TS_ASSERT_EQUALS(0U, stats.numberOfRecords_);
		TS_ASSERT_EQUALS(0U, stats.dataInlineSize_);
	}

	void checkAndRemoveRecordOfSize(RecordsIteratedOver& records, const std::string& key, partNum_t partNum, size_t valueSize, unsigned seed = 30472)
	{
		const std::string value = valueOfSize(valueSize, seed);
		TS_ASSERT_THROWS_NOTHING(records.checkAndRemove(key, partNum, value));
	}

	//-------------------------------------------------------------------------

	void storeValueOfSize(Database db, const std::string& key, partNum_t partNum, size_t valueSize, unsigned seed = 30472)
	{
		const std::string value = valueOfSize(valueSize, seed);
		PRINT_EXCEPTION(db->store(key, partNum, value));
	}

	//-------------------------------------------------------------------------

	void checkRecordDoesNotExist(Database db, const std::string& key, partNum_t partNum)
	{
		std::string nonExistentValue;
		TS_ASSERT(! db->fetch(key, partNum, nonExistentValue));
	}

	void checkRecordValue(Database db, const std::string& key, partNum_t partNum, const std::string& expectedValue)
	{
		std::string readValue;
		TS_ASSERT(db->fetch(key, partNum, readValue));
		TS_ASSERT_EQUALS(expectedValue, readValue);
	}

	void checkRecordValueOfSize(Database db, const std::string& key, partNum_t partNum, size_t valueSize, unsigned seed = 30472)
	{
		const std::string expectedValue = valueOfSize(valueSize, seed);
		TS_ASSERT_THROWS_NOTHING(checkRecordValue(db, key, partNum, expectedValue));
	}

};

//-----------------------------------------------------------------------------

void DatabaseTest::testSingleInlineValue()
{
	const std::string name = databaseTestPath_ + "/db";

	const std::string key("1");
	const partNum_t partNumber = 33;
	const std::string value = valueOfSize(456);

	// Create a database with single record.
	{
		// Open.
		Database db = DatabaseFactory();
		TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

		Statistics statsEmpty = db->statistics();
		TS_ASSERT_EQUALS(1U, statsEmpty.bucketPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bucketPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.splitsOnOverfill_);
		TS_ASSERT_EQUALS(defaultPageSize(), statsEmpty.pageSize_);
		TS_ASSERT_EQUALS(1U, statsEmpty.numberOfBuckets_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowFileDataPages_);
		TS_ASSERT_EQUALS(0U, statsEmpty.numberOfRecords_);
		TS_ASSERT_EQUALS(0U, statsEmpty.dataInlineSize_);

		// Store.
		TS_ASSERT_THROWS_NOTHING(db->store(key, partNumber, value));

		// Check.
		std::string readValue;
		TS_ASSERT(db->fetch(key, partNumber, readValue));
		TS_ASSERT_EQUALS(value, readValue);

		RecordsIteratedOver recordList(db);
		TS_ASSERT_EQUALS(1U, recordList.size());
		TS_ASSERT_THROWS_NOTHING(recordList.checkAndRemove(key, partNumber, value));
		TS_ASSERT(recordList.empty());

		std::vector<partNum_t> partVec = db->listParts(key);
		std::vector<partNum_t> expectedPartVec;
		expectedPartVec.push_back(partNumber);
		TS_ASSERT_EQUALS(expectedPartVec, partVec);

		Statistics statsSingleRecord = db->statistics();
		TS_ASSERT_EQUALS(1U, statsSingleRecord.bucketPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.overflowPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.largeValuePagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.bitmapPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.bucketPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.overflowPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.largeValuePagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.bitmapPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.splitsOnOverfill_);
		TS_ASSERT_EQUALS(defaultPageSize(), statsSingleRecord.pageSize_);
		TS_ASSERT_EQUALS(1U, statsSingleRecord.numberOfBuckets_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.overflowFileDataPages_);
		TS_ASSERT_EQUALS(1U, statsSingleRecord.numberOfRecords_);
		TS_ASSERT(statsSingleRecord.dataInlineSize_ > value.size());

		TS_ASSERT_THROWS_NOTHING(db->flush());
		TS_ASSERT_THROWS_NOTHING(db->close());
	}

	// Check the record and delete it.
	{
		Database db = DatabaseFactory();
		TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

		// Check the value.
		std::string readValue;
		TS_ASSERT(db->fetch(key, partNumber, readValue));
		TS_ASSERT_EQUALS(value, readValue);

		RecordsIteratedOver recordList(db);
		TS_ASSERT_EQUALS(1U, recordList.size());
		TS_ASSERT_THROWS_NOTHING(recordList.checkAndRemove(key, partNumber, value));
		TS_ASSERT(recordList.empty());

		std::vector<partNum_t> partVec = db->listParts(key);
		std::vector<partNum_t> expectedPartVec;
		expectedPartVec.push_back(partNumber);
		TS_ASSERT_EQUALS(expectedPartVec, partVec);

		Statistics statsSingleRecord = db->statistics();
		TS_ASSERT_EQUALS(0U, statsSingleRecord.bucketPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.overflowPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.largeValuePagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.bitmapPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.bucketPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.overflowPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.largeValuePagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.bitmapPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.splitsOnOverfill_);
		TS_ASSERT_EQUALS(defaultPageSize(), statsSingleRecord.pageSize_);
		TS_ASSERT_EQUALS(1U, statsSingleRecord.numberOfBuckets_);
		TS_ASSERT_EQUALS(0U, statsSingleRecord.overflowFileDataPages_);
		TS_ASSERT_EQUALS(1U, statsSingleRecord.numberOfRecords_);
		TS_ASSERT(statsSingleRecord.dataInlineSize_ > value.size());

		// Delete.
		TS_ASSERT_THROWS_NOTHING(db->remove(key, partNumber));

		// Check that record is deleted.
		TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, partNumber));

		TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

		std::vector<partNum_t> emptyPartVec = db->listParts(key);
		TS_ASSERT(emptyPartVec.empty());

		Statistics statsEmpty = db->statistics();
		TS_ASSERT_EQUALS(0U, statsEmpty.bucketPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bucketPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.splitsOnOverfill_);
		TS_ASSERT_EQUALS(defaultPageSize(), statsEmpty.pageSize_);
		TS_ASSERT_EQUALS(1U, statsEmpty.numberOfBuckets_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowFileDataPages_);

		TS_ASSERT_THROWS_NOTHING(db->close());
	}

	// Check that there is no value.
	{
		Database db = DatabaseFactory();
		TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

		// Check that record is deleted.
		TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, partNumber));
		TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

		std::vector<partNum_t> emptyPartVec = db->listParts(key);
		TS_ASSERT(emptyPartVec.empty());

		Statistics statsEmpty = db->statistics();
		TS_ASSERT_EQUALS(0U, statsEmpty.bucketPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesAcquired_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bucketPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesReleased_);
		TS_ASSERT_EQUALS(0U, statsEmpty.splitsOnOverfill_);
		TS_ASSERT_EQUALS(defaultPageSize(), statsEmpty.pageSize_);
		TS_ASSERT_EQUALS(1U, statsEmpty.numberOfBuckets_);
		TS_ASSERT_EQUALS(0U, statsEmpty.overflowFileDataPages_);

		TS_ASSERT_THROWS_NOTHING(db->close());
	}
}

//-----------------------------------------------------------------------------

namespace {

	void doTestFillSinglePage(Database db, const std::string& name, size_type pageSize)
	{
		const std::string key("1");
		const size_type initialRecordSize = static_cast<size_type>(key.size()) + 2 + 2 + 3; // Size of the record is 1 (key len) + 1 (key) + 1 (part num) + 2 (value len) + 3 (value size)
		const size_type initialNumberOfRecords = std::min((pageSize - 20) / (initialRecordSize + 2), 128U);

		const size_type replacementRecordSize = static_cast<size_type>(key.size()) + 2 + 2 + 2;
		const size_type replacementNumberOfRecords = std::min((pageSize - 20) / (replacementRecordSize + 2), 128U);

		// Create records.
		{
			Options options = Options::readWriteSingleThreaded();
			options.pageSize_ = pageSize;
			TS_ASSERT_THROWS_NOTHING(db->open(name, options));

			// Store.
			for (partNum_t part = 0; part < static_cast<partNum_t>(initialNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, part, 3, 22 + part));
			}

			// Check.
			for (partNum_t part = 0; part < static_cast<partNum_t>(initialNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, 3, 22 + part));
			}

			RecordsIteratedOver records(db);
			TS_ASSERT_EQUALS(initialNumberOfRecords, records.size());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			for (partNum_t part = 0; part < static_cast<partNum_t>(initialNumberOfRecords); ++part) {
				expectedPartVec.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(1U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(0U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(0U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(initialNumberOfRecords, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(initialNumberOfRecords * initialRecordSize, stats.dataInlineSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Check and overwrite with smaller records.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Check.
			for (partNum_t part = 0; part < static_cast<partNum_t>(initialNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, 3, 22 + part));
			}

			RecordsIteratedOver records(db);
			TS_ASSERT_EQUALS(initialNumberOfRecords, records.size());

			std::vector<partNum_t> initialPartVec = db->listParts(key);
			TS_ASSERT_EQUALS(initialNumberOfRecords, initialPartVec.size());

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(0U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(0U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(initialNumberOfRecords, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(initialNumberOfRecords * initialRecordSize, stats.dataInlineSize_);

			// Store (more) smaller records.
			for (partNum_t part = 0; part < static_cast<partNum_t>(replacementNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, part, 2, 44 + part));
			}

			// Check.
			for (partNum_t part = 0; part < static_cast<partNum_t>(replacementNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, 2, 44 + part));
			}

			RecordsIteratedOver replacementRecords(db);
			TS_ASSERT_EQUALS(replacementNumberOfRecords, replacementRecords.size());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			for (partNum_t part = 0; part < static_cast<partNum_t>(replacementNumberOfRecords); ++part) {
				expectedPartVec.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(replacementNumberOfRecords, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(replacementNumberOfRecords * replacementRecordSize, stats.dataInlineSize_);
			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Check and delete.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Check.
			for (partNum_t part = 0; part < static_cast<partNum_t>(replacementNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, 2, 44 + part));
			}

			RecordsIteratedOver records(db);
			TS_ASSERT_EQUALS(replacementNumberOfRecords, records.size());

			std::vector<partNum_t> initialPartVec = db->listParts(key);
			TS_ASSERT_EQUALS(replacementNumberOfRecords, initialPartVec.size());

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(0U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(0U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(replacementNumberOfRecords, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(replacementNumberOfRecords * replacementRecordSize, stats.dataInlineSize_);

			// Delete.
			for (partNum_t part = 0; part < static_cast<partNum_t>(replacementNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(db->remove(key, part));
			}

			for (partNum_t part = 0; part < static_cast<partNum_t>(replacementNumberOfRecords); ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, part));
			}

			TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

			std::vector<partNum_t> partVec = db->listParts(key);
			TS_ASSERT(partVec.empty());

			TS_ASSERT_THROWS_NOTHING(db->close());
		}
	}

};

void DatabaseTest::testFillSinglePage()
{
	Database db = DatabaseFactory();

	TS_ASSERT_THROWS_NOTHING(doTestFillSinglePage(db, databaseTestPath_ + "/dbMin", MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestFillSinglePage(db, databaseTestPath_ + "/dbMax", MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestCreateThreeOverflowPages(Database db, const std::string& name, size_type pageSize)
	{
		const size_type largestInlineRecordSize = pageSize - (20 /* HEADER_DATA_END_OFFSET */ + sizeof(uint16_t));
		const size_type valueSize = largestInlineRecordSize - 5; // key size (1) + key (1) + part number (1) + value size (2) = 5

		const std::string key("6");
		const partNum_t numberOfRecords = 4; // 1 on bucket page (+ 2 empty bucket pages) and 3 on overflow pages.

		Options options = Options::readWriteSingleThreaded();
		options.pageSize_ = pageSize;
		options.leavePageFreeSpace_ -= largestInlineRecordSize * numberOfRecords; // Disable page split triggered by overfill.

		// Create four large inline values.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, options));

			// Store.
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, part, valueSize, part + 333));
			}

			// Check.
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, valueSize, part + 333));
			}

			RecordsIteratedOver records(db);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords), records.size());
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, part, valueSize, part + 333));
			}
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				expectedPartVec.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(3U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(4U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(1U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(1U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(3U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords), stats.numberOfRecords_);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords * largestInlineRecordSize), stats.dataInlineSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Check values and delete the first one.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Check.
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, valueSize, part + 333));
			}

			RecordsIteratedOver recordsBeforeDelete(db);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords), recordsBeforeDelete.size());
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeDelete, key, part, valueSize, part + 333));
			}
			TS_ASSERT(recordsBeforeDelete.empty());

			std::vector<partNum_t> partVecBeforeDelete = db->listParts(key);
			std::vector<partNum_t> expectedPartVecBeforeDelete;
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				expectedPartVecBeforeDelete.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVecBeforeDelete, partVecBeforeDelete);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(3U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords), stats.numberOfRecords_);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords * largestInlineRecordSize), stats.dataInlineSize_);

			// Delete first record.
			TS_ASSERT_THROWS_NOTHING(db->remove(key, 0));

			// Check.
			for (partNum_t part = 1; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, valueSize, part + 333));
			}

			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 0));

			RecordsIteratedOver recordsAfterDelete(db);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords - 1), recordsAfterDelete.size());
			for (partNum_t part = 1; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsAfterDelete, key, part, valueSize, part + 333));
			}
			TS_ASSERT(recordsBeforeDelete.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			for (partNum_t part = 1; part < numberOfRecords; ++part) {
				expectedPartVec.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(3U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords - 1), stats.numberOfRecords_);
			TS_ASSERT_EQUALS(static_cast<size_type>((numberOfRecords - 1) * largestInlineRecordSize), stats.dataInlineSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Check values and delete the last ones.
		for (partNum_t maxPart = numberOfRecords - 1; maxPart != 0; --maxPart)
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Check.
			for (partNum_t part = 1; part < numberOfRecords; ++part) {
				if (part <= maxPart) {
					TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, valueSize, part + 333));
				}
				else {
					TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, part));
				}
			}

			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 0));

			RecordsIteratedOver records(db);
			for (partNum_t part = 1; part <= maxPart; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, part, valueSize, part + 333));
			}
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVecBeforeDelete = db->listParts(key);
			std::vector<partNum_t> expectedPartVecBeforeDelete;
			for (partNum_t part = 1; part <= maxPart; ++part) {
				expectedPartVecBeforeDelete.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVecBeforeDelete, partVecBeforeDelete);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(3U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(static_cast<size_type>(maxPart), stats.numberOfRecords_);
			TS_ASSERT_EQUALS(static_cast<size_type>(maxPart * largestInlineRecordSize), stats.dataInlineSize_);

			// Delete and check.
			TS_ASSERT_THROWS_NOTHING(db->remove(key, maxPart));

			for (partNum_t part = 1; part < numberOfRecords; ++part) {
				if (part <= maxPart - 1) {
					TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, valueSize, part + 333));
				}
				else {
					TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, part));
				}
			}

			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 0));

			RecordsIteratedOver recordsAfterDelete(db);
			for (partNum_t part = 1; part <= maxPart - 1; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsAfterDelete, key, part, valueSize, part + 333));
			}
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVecAfterDelete = db->listParts(key);
			std::vector<partNum_t> expectedPartVecAfterDelete;
			for (partNum_t part = 1; part <= maxPart - 1; ++part) {
				expectedPartVecAfterDelete.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVecAfterDelete, partVecAfterDelete);

			stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(3U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(static_cast<size_type>(maxPart - 1), stats.numberOfRecords_);
			TS_ASSERT_EQUALS(static_cast<size_type>((maxPart - 1) * largestInlineRecordSize), stats.dataInlineSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Reopen and check that database is empty.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readOnlySingleThreaded()));
			TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

			std::vector<partNum_t> emptyPartVecAfterDelete = db->listParts(key);
			TS_ASSERT(emptyPartVecAfterDelete.empty());

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(3U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Add some large values. This tests possible overwriting of overflow pages with large value pages.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Store.
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, part, 2*valueSize, part + 555));
			}

			// Check.
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, part, 2*valueSize, part + 555));
			}

			RecordsIteratedOver records(db);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords), records.size());
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, part, 2*valueSize, part + 555));
			}
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			for (partNum_t part = 0; part < numberOfRecords; ++part) {
				expectedPartVec.push_back(part);
			}
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(8U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(3U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(11U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords), stats.numberOfRecords_);
			TS_ASSERT_EQUALS(static_cast<size_type>(numberOfRecords * 13), stats.dataInlineSize_); // 3 * (key size (1) + key (1) + part number (1) + value size/tag (2) + value size (4) + first large value pointer (4)).

			TS_ASSERT_THROWS_NOTHING(db->close());
		}
	}

}; // namespace

void DatabaseTest::testCreateThreeOverflowPages()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestCreateThreeOverflowPages(db, minPageDatabaseName, MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestCreateThreeOverflowPages(db, maxPageDatabaseName, MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestCreateThreePageLargeValue(Database db, const std::string& name, size_type pageSize)
	{
		const std::string key("6");

		const size_type threePageLargeValueSize = 3 * (pageSize - 16 /* HEADER_DATA_END_OFFSET */);

		const size_type inlineRecordSize = (pageSize - (20 /* HEADER_DATA_END_OFFSET */ + sizeof(uint16_t)) - 13 /* large record size */) / 2;
		const size_type inlineValueSize = inlineRecordSize - 7; // key size (1) + key (1) + part number (1) + value size (2) + pointer to value = 7

		const partNum_t singleLargeValuePartNumber = 3; // Used in the first part of the test.

		// Create a single three-page large value.
		{
			Options options = Options::readWriteSingleThreaded();
			options.pageSize_ = pageSize;

			TS_ASSERT_THROWS_NOTHING(db->open(name, options));

			// Store.
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, singleLargeValuePartNumber, threePageLargeValueSize));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, singleLargeValuePartNumber, threePageLargeValueSize));

			RecordsIteratedOver records(db);
			TS_ASSERT_EQUALS(1U, records.size());
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, singleLargeValuePartNumber, threePageLargeValueSize));
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			TS_ASSERT_EQUALS(1U, partVec.size());
			TS_ASSERT_EQUALS(singleLargeValuePartNumber, partVec[0]);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(1U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(3U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(1U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(1U, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(13U, stats.dataInlineSize_); // key size (1) + key (1) + part number (1) + value size/tag (2) + value size (4) + first large value pointer (4).

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Open and test that the value exists.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readOnlySingleThreaded()));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, singleLargeValuePartNumber, threePageLargeValueSize));

			RecordsIteratedOver records(db);
			TS_ASSERT_EQUALS(1U, records.size());
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, singleLargeValuePartNumber, threePageLargeValueSize));
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			TS_ASSERT_EQUALS(1U, partVec.size());
			TS_ASSERT_EQUALS(singleLargeValuePartNumber, partVec[0]);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(1U, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(13U, stats.dataInlineSize_); // key size (1) + key (1) + part number (1) + value size/tag (2) + value size (4) + first large value pointer (4).

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Delete the large value
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Delete.
			TS_ASSERT_THROWS_NOTHING(db->remove(key, singleLargeValuePartNumber));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, singleLargeValuePartNumber));

			RecordsIteratedOver records(db);
			TS_ASSERT(records.empty());

			std::vector<partNum_t> emptyPartVec = db->listParts(key);
			TS_ASSERT(emptyPartVec.empty());

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(3U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(0U, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(0U, stats.dataInlineSize_); // key size (1) + key (1) + part number (1) + value size/tag (2) + value size (4) + first large value pointer (4).

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Reopen and check that database is empty. 
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readOnlySingleThreaded()));
			TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

			std::vector<partNum_t> emptyPartVec = db->listParts(key);
			TS_ASSERT(emptyPartVec.empty());

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// To bucket page and to overflow page add a large value between 2 inline values.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Store.
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 2, inlineValueSize));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));

			RecordsIteratedOver records(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 2, inlineValueSize));
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			expectedPartVec.push_back(0);
			expectedPartVec.push_back(1);
			expectedPartVec.push_back(2);
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(3U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(3U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(3U, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(13U + 2 * (inlineRecordSize - sizeof(uint16_t)), stats.dataInlineSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Add a large value to overflow page between 2 inline values.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));

			RecordsIteratedOver bucketPageRecords(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(bucketPageRecords, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(bucketPageRecords, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(bucketPageRecords, key, 2, inlineValueSize));
			TS_ASSERT(bucketPageRecords.empty());

			std::vector<partNum_t> bucketPagePartVec = db->listParts(key);
			std::vector<partNum_t> bucketPageExpectedParts;
			bucketPageExpectedParts.push_back(0);
			bucketPageExpectedParts.push_back(1);
			bucketPageExpectedParts.push_back(2);
			TS_ASSERT_EQUALS(bucketPageExpectedParts, bucketPagePartVec);

			// Store to overflow page.
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 5, inlineValueSize));

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			expectedPartVec.push_back(0);
			expectedPartVec.push_back(1);
			expectedPartVec.push_back(2);
			expectedPartVec.push_back(3);
			expectedPartVec.push_back(4);
			expectedPartVec.push_back(5);
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 5, inlineValueSize));

			RecordsIteratedOver records(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 5, inlineValueSize));
			TS_ASSERT(records.empty());

			partVec = db->listParts(key);
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(1U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(1U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(3U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(7U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(6U, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(2 * (13U + 2 * (inlineRecordSize - sizeof(uint16_t))), stats.dataInlineSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Update the large values and check that all values still read correctly.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 5, inlineValueSize));

			RecordsIteratedOver recordsBeforeUpdate(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeUpdate, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeUpdate, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeUpdate, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeUpdate, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeUpdate, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeUpdate, key, 5, inlineValueSize));
			TS_ASSERT(recordsBeforeUpdate.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			expectedPartVec.push_back(0);
			expectedPartVec.push_back(1);
			expectedPartVec.push_back(2);
			expectedPartVec.push_back(3);
			expectedPartVec.push_back(4);
			expectedPartVec.push_back(5);
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			// Overwrite large values with smaller ones.
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 1, threePageLargeValueSize - 1));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 4, threePageLargeValueSize - 2));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, threePageLargeValueSize - 1));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 4, threePageLargeValueSize - 2));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 5, inlineValueSize));

			RecordsIteratedOver records(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 1, threePageLargeValueSize - 1));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 4, threePageLargeValueSize - 2));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 5, inlineValueSize));
			TS_ASSERT(records.empty());

			partVec = db->listParts(key);
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(6U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(6U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(7U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(6U, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(2 * (13U + 2 * (inlineRecordSize - sizeof(uint16_t))), stats.dataInlineSize_); // Inline size did not change.

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Delete the large values and check that inline values read correctly. Then delete inline values.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, threePageLargeValueSize - 1));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 4, threePageLargeValueSize - 2));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 5, inlineValueSize));

			RecordsIteratedOver recordsBeforeDelete(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeDelete, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeDelete, key, 1, threePageLargeValueSize - 1));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeDelete, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeDelete, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeDelete, key, 4, threePageLargeValueSize - 2));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(recordsBeforeDelete, key, 5, inlineValueSize));
			TS_ASSERT(recordsBeforeDelete.empty());

			std::vector<partNum_t> partVecBeforeDelete = db->listParts(key);
			std::vector<partNum_t> expectedPartVecBeforeDelete;
			expectedPartVecBeforeDelete.push_back(0);
			expectedPartVecBeforeDelete.push_back(1);
			expectedPartVecBeforeDelete.push_back(2);
			expectedPartVecBeforeDelete.push_back(3);
			expectedPartVecBeforeDelete.push_back(4);
			expectedPartVecBeforeDelete.push_back(5);
			TS_ASSERT_EQUALS(expectedPartVecBeforeDelete, partVecBeforeDelete);

			// Delete both large values.
			TS_ASSERT_THROWS_NOTHING(db->remove(key, 1));
			TS_ASSERT_THROWS_NOTHING(db->remove(key, 4));

			// Check.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 1));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 4));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 5, inlineValueSize));

			RecordsIteratedOver records(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 5, inlineValueSize));
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			expectedPartVec.push_back(0);
			expectedPartVec.push_back(2);
			expectedPartVec.push_back(3);
			expectedPartVec.push_back(5);
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(6U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(7U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(4U, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(2 * (2 * (inlineRecordSize - sizeof(uint16_t))), stats.dataInlineSize_);

			// Delete inline values.
			TS_ASSERT_THROWS_NOTHING(db->remove(key, 0));
			TS_ASSERT_THROWS_NOTHING(db->remove(key, 2));
			TS_ASSERT_THROWS_NOTHING(db->remove(key, 3));
			TS_ASSERT_THROWS_NOTHING(db->remove(key, 5));

			TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

			std::vector<partNum_t> emptyPartVec = db->listParts(key);
			TS_ASSERT(emptyPartVec.empty());

			stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(6U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(7U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Reopen and check that database is empty. 
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readOnlySingleThreaded()));
			TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

			std::vector<partNum_t> emptyPartVec = db->listParts(key);
			TS_ASSERT(emptyPartVec.empty());

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(7U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}
	}

};

void DatabaseTest::testCreateThreePageLargeValue()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestCreateThreePageLargeValue(db, minPageDatabaseName, MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestCreateThreePageLargeValue(db, maxPageDatabaseName, MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestRemoveAllParts(Database db, const std::string& name, size_type pageSize)
	{
		const std::string key("6");

		const size_type threePageLargeValueSize = 3 * (pageSize - 16 /* HEADER_DATA_END_OFFSET */);

		const size_type inlineRecordSize = (pageSize - (20 /* HEADER_DATA_END_OFFSET */ + sizeof(uint16_t)) - 13 /* large record size */) / 2;
		const size_type inlineValueSize = inlineRecordSize - 7; // key size (1) + key (1) + part number (1) + value size (2) + pointer to value = 7

		{
			Options options = Options::readWriteSingleThreaded();
			options.pageSize_ = pageSize;

			TS_ASSERT_THROWS_NOTHING(db->open(name, options));

			// Insert 3 values to bucket page.
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 2, inlineValueSize));

			// Check that bucket and large values pages were used.
			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(1U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(3U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);

			// Insert 3 values to overflow page.
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 5, inlineValueSize));

			// Check that overflow and large values pages were used.
			stats = db->statistics();
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(1U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(6U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);

			// Check that all values are present.
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 5, inlineValueSize));

			RecordsIteratedOver records(db);
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 0, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 1, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 2, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 3, inlineValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 4, threePageLargeValueSize));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, 5, inlineValueSize));
			TS_ASSERT(records.empty());

			std::vector<partNum_t> partVec = db->listParts(key);
			std::vector<partNum_t> expectedPartVec;
			expectedPartVec.push_back(0);
			expectedPartVec.push_back(1);
			expectedPartVec.push_back(2);
			expectedPartVec.push_back(3);
			expectedPartVec.push_back(4);
			expectedPartVec.push_back(5);
			TS_ASSERT_EQUALS(expectedPartVec, partVec);

			// Delete all.
			TS_ASSERT_THROWS_NOTHING(db->remove(key));

			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 0));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 1));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 2));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 3));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 4));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 5));

			TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

			std::vector<partNum_t> emptyPartVec = db->listParts(key);
			TS_ASSERT(emptyPartVec.empty());

			stats = db->statistics();
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(1U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(6U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(6U, stats.largeValuePagesReleased_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Reopen and check that database is empty. 
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readOnlySingleThreaded()));
			TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

			std::vector<partNum_t> emptyPartVec = db->listParts(key);
			TS_ASSERT(emptyPartVec.empty());

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(7U, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

	}
};

void DatabaseTest::testRemoveAllParts()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestRemoveAllParts(db, minPageDatabaseName, MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestRemoveAllParts(db, maxPageDatabaseName, MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestCreatePreallocatedDatabase(Database db, const std::string& name, size_type pageSize)
	{
		const size_type inlineRecordSize = (pageSize - (20 /* HEADER_DATA_END_OFFSET */ + sizeof(uint16_t)));
		const size_type inlineValueSize = inlineRecordSize - 4; // key size (1) + key (not included) + part number (1) + value size (2) = 4
		const size_type maxNumberOfRecords = 13;

		std::vector<std::string> keys = nonCollidingKeys(maxNumberOfRecords);
		TS_ASSERT_EQUALS(maxNumberOfRecords, keys.size());

		for (size_type numberOfRecords = 1; numberOfRecords < maxNumberOfRecords; ++ numberOfRecords) {

			{
				Options options = Options::readWriteSingleThreaded();
				options.pageSize_ = pageSize;
				options.initialBuckets_ = numberOfRecords;

				TS_ASSERT_THROWS_NOTHING(db->open(name, options));

				// Fill all buckets.
				for (size_type i = 0; i < numberOfRecords; ++i) {
					const size_type valueSize = inlineValueSize - static_cast<size_type>(keys[i].size());
					TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, keys[i], 0, valueSize));
				}

				// Check.
				for (size_type i = 0; i < numberOfRecords; ++i) {
					const size_type valueSize = inlineValueSize - static_cast<size_type>(keys[i].size());
					TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, keys[i], 0, valueSize));
				}

				RecordsIteratedOver records(db);
				for (size_type i = 0; i < numberOfRecords; ++i) {
					const size_type valueSize = inlineValueSize - static_cast<size_type>(keys[i].size());
					TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, keys[i], 0, valueSize));
				}
				TS_ASSERT(records.empty());

				for (size_type i = 0; i < numberOfRecords; ++i) {
					std::vector<partNum_t> partVec = db->listParts(keys[i]);
					TS_ASSERT_EQUALS(1U, partVec.size());
					TS_ASSERT_EQUALS(0U, partVec[0]);
				}

				Statistics stats = db->statistics();
				TS_ASSERT_EQUALS(numberOfRecords, stats.bucketPagesAcquired_);
				TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
				TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
				TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
				TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
				TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
				TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
				TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
				TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
				TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
				TS_ASSERT_EQUALS(numberOfRecords, stats.numberOfBuckets_);
				TS_ASSERT_EQUALS(0U, stats.overflowFileDataPages_);
				TS_ASSERT_EQUALS(0U, stats.overflowFileBitmapPages_);
				TS_ASSERT_EQUALS(numberOfRecords, stats.numberOfRecords_);
				TS_ASSERT_EQUALS(numberOfRecords * inlineRecordSize, stats.dataInlineSize_);

				TS_ASSERT_THROWS_NOTHING(db->close());
			}

			{
				// Reopen the database, check and delete.
				TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

				// Check.
				for (size_type i = 0; i < numberOfRecords; ++i) {
					const size_type valueSize = inlineValueSize - static_cast<size_type>(keys[i].size());
					TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, keys[i], 0, valueSize));
				}

				RecordsIteratedOver records(db);
				for (size_type i = 0; i < numberOfRecords; ++i) {
					const size_type valueSize = inlineValueSize - static_cast<size_type>(keys[i].size());
					TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, keys[i], 0, valueSize));
				}
				TS_ASSERT(records.empty());

				for (size_type i = 0; i < numberOfRecords; ++i) {
					std::vector<partNum_t> partVec = db->listParts(keys[i]);
					TS_ASSERT_EQUALS(1U, partVec.size());
					TS_ASSERT_EQUALS(0U, partVec[0]);
				}

				// Remove and check.
				for (size_type i = 0; i < numberOfRecords; ++i) {
					TS_ASSERT_THROWS_NOTHING(db->remove(keys[i], 0));
				}

				for (size_type i = 0; i < numberOfRecords; ++i) {
					TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, keys[i], 0));

					std::vector<partNum_t> emptyPartVec = db->listParts(keys[i]);
					TS_ASSERT(emptyPartVec.empty());
				}

				TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

				Statistics statsEmpty = db->statistics();
				TS_ASSERT_EQUALS(0U, statsEmpty.bucketPagesAcquired_);
				TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesAcquired_);
				TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesAcquired_);
				TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesAcquired_);
				TS_ASSERT_EQUALS(0U, statsEmpty.bucketPagesReleased_);
				TS_ASSERT_EQUALS(0U, statsEmpty.overflowPagesReleased_);
				TS_ASSERT_EQUALS(0U, statsEmpty.largeValuePagesReleased_);
				TS_ASSERT_EQUALS(0U, statsEmpty.bitmapPagesReleased_);
				TS_ASSERT_EQUALS(0U, statsEmpty.splitsOnOverfill_);
				TS_ASSERT_EQUALS(pageSize, statsEmpty.pageSize_);
				TS_ASSERT_EQUALS(numberOfRecords, statsEmpty.numberOfBuckets_);
				TS_ASSERT_EQUALS(0U, statsEmpty.overflowFileDataPages_);
				TS_ASSERT_EQUALS(0U, statsEmpty.overflowFileBitmapPages_);

				TS_ASSERT_THROWS_NOTHING(db->close());
			}

			TS_ASSERT(db->drop(name));
		}
	}

};

void DatabaseTest::testCreatePreallocatedDatabase()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestCreatePreallocatedDatabase(db, minPageDatabaseName, MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestCreatePreallocatedDatabase(db, maxPageDatabaseName, MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestBucketSplit(Database db, const std::string& name, size_type pageSize, size_type maxNumberOfRecords)
	{
		const size_type inlineRecordSize = (pageSize - (20 /* HEADER_DATA_END_OFFSET */ + sizeof(uint16_t)));
		const size_type inlineValueSize = inlineRecordSize - 4; // key size (1) + key (not included) + part number (1) + value size (2) = 4

		// Create.
		{
			Options options = Options::readWriteSingleThreaded();
			options.pageSize_ = pageSize;

			TS_ASSERT_THROWS_NOTHING(db->open(name, options));

			for (unsigned numberOfRecords = 1; numberOfRecords < maxNumberOfRecords; ++numberOfRecords) {
				const std::string key(keyFor(numberOfRecords));
				const size_type valueSize = inlineValueSize - static_cast<size_type>(key.size());
				TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 0, valueSize, numberOfRecords));
			
				// Check.
				for (unsigned i = 1; i <= numberOfRecords; ++i) {
					const std::string checkedKey(keyFor(i));
					const size_type checkedValueSize = inlineValueSize - static_cast<size_type>(checkedKey.size());
					TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, checkedKey, 0, checkedValueSize, i));
				}

				RecordsIteratedOver records(db);
				for (unsigned i = 1; i <= numberOfRecords; ++i) {
					const std::string recordKey(keyFor(i));
					const size_type recordValueSize = inlineValueSize - static_cast<size_type>(recordKey.size());
					TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, recordKey, 0, recordValueSize, i));
				}
				TS_ASSERT(records.empty());

				Statistics stats = db->statistics();
				TS_ASSERT(numberOfRecords >= stats.bucketPagesAcquired_);
				TS_ASSERT_EQUALS(stats.numberOfBuckets_, stats.bucketPagesAcquired_);
				TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);

				const size_type expectedBitmapPageAcquisitions = (stats.overflowPagesAcquired_)? 1 : 0;
				TS_ASSERT_EQUALS(expectedBitmapPageAcquisitions, stats.bitmapPagesAcquired_);

				TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
				TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
				TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
				TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
				TS_ASSERT_EQUALS(expectedBitmapPageAcquisitions, stats.overflowFileBitmapPages_);
				TS_ASSERT_EQUALS(numberOfRecords, stats.numberOfRecords_);
				TS_ASSERT_EQUALS(numberOfRecords * inlineRecordSize, stats.dataInlineSize_);
			}

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Check and delete.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			for (unsigned recordNumber = 1; recordNumber < maxNumberOfRecords; ++recordNumber) {
				const std::string key(keyFor(recordNumber));
				const size_type valueSize = inlineValueSize - static_cast<size_type>(key.size());
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, valueSize, recordNumber));

				RecordsIteratedOver records(db);
				for (unsigned i = 1; i < maxNumberOfRecords; ++i) {
					const std::string recordKey(keyFor(i));
					const size_type recordValueSize = inlineValueSize - static_cast<size_type>(recordKey.size());
					TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, recordKey, 0, recordValueSize, i));
				}
				TS_ASSERT(records.empty());
			}

			for (unsigned recordNumber = 1; recordNumber < maxNumberOfRecords; ++recordNumber) {
				const std::string key(keyFor(recordNumber));
				TS_ASSERT_THROWS_NOTHING(db->remove(key, 0));
				TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 0));

				RecordsIteratedOver records(db);
				for (unsigned i = recordNumber + 1; i < maxNumberOfRecords; ++i) {
					const std::string recordKey(keyFor(i));
					const size_type recordValueSize = inlineValueSize - static_cast<size_type>(recordKey.size());
					TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, recordKey, 0, recordValueSize, i));
				}
				TS_ASSERT(records.empty());
			}

			checkDatabaseIsEmpty(db);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_); // Overflow pages are not yet released.
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}
	}
};

void DatabaseTest::testBucketSplit()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestBucketSplit(db, minPageDatabaseName, MIN_PAGE_SIZE, 97));
	TS_ASSERT_THROWS_NOTHING(doTestBucketSplit(db, maxPageDatabaseName, MAX_PAGE_SIZE, 23));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestBucketSplitLargeValues(Database db, const std::string& name, size_type pageSize)
	{
		const size_type largeRecordValueSize = pageSize;
		const size_type largeRecordInlineSize = 12; // key size (1) + key (not included) + part number (1) + value size (2) + 8 (2 * uint32_t)
		unsigned numberOfRecords = 0;

		// Create.
		{
			Options options = Options::readWriteSingleThreaded();
			options.pageSize_ = pageSize;

			TS_ASSERT_THROWS_NOTHING(db->open(name, options));

			bool isSplit = false;
			size_t keySize = 0;

			while (! isSplit) {
				const std::string key(keyFor(numberOfRecords));
				keySize += key.size();

				TS_ASSERT_THROWS_NOTHING(storeValueOfSize(db, key, 0, largeRecordValueSize, numberOfRecords));
				++numberOfRecords;

				// Check.
				for (unsigned i = 0; i < numberOfRecords; ++i) {
					const std::string checkedKey(keyFor(i));
					TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, checkedKey, 0, largeRecordValueSize, i));
				}

				RecordsIteratedOver records(db);
				for (unsigned i = 0; i < numberOfRecords; ++i) {
					const std::string recordKey(keyFor(i));
					TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, recordKey, 0, largeRecordValueSize, i));
				}
				TS_ASSERT(records.empty());

				Statistics stats = db->statistics();
				isSplit = (stats.bucketPagesAcquired_ > 1);
			}

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(2U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(2 * numberOfRecords, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(1U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(2 * numberOfRecords, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);
			TS_ASSERT_EQUALS(numberOfRecords, stats.numberOfRecords_);
			TS_ASSERT_EQUALS(keySize + (numberOfRecords * largeRecordInlineSize), stats.dataInlineSize_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}

		// Check and delete.
		{
			TS_ASSERT_THROWS_NOTHING(db->open(name, Options::readWriteSingleThreaded()));

			for (unsigned recordNumber = 0; recordNumber < numberOfRecords; ++recordNumber) {
				const std::string checkedKey(keyFor(recordNumber));
				TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, checkedKey, 0, largeRecordValueSize, recordNumber));
			}

			RecordsIteratedOver records(db);
			for (unsigned recordNumber = 0; recordNumber < numberOfRecords; ++recordNumber) {
				const std::string recordKey(keyFor(recordNumber));
				TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, recordKey, 0, largeRecordValueSize, recordNumber));
			}
			TS_ASSERT(records.empty());
			
			for (unsigned recordNumber = 0; recordNumber < numberOfRecords; ++recordNumber) {
				const std::string key(keyFor(recordNumber));
				TS_ASSERT_THROWS_NOTHING(db->remove(key, 0));
				TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 0));
			}

			checkDatabaseIsEmpty(db);

			Statistics stats = db->statistics();
			TS_ASSERT_EQUALS(0U, stats.bucketPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.largeValuePagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesAcquired_);
			TS_ASSERT_EQUALS(0U, stats.bucketPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.overflowPagesReleased_);
			TS_ASSERT_EQUALS(2 * numberOfRecords, stats.largeValuePagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.bitmapPagesReleased_);
			TS_ASSERT_EQUALS(0U, stats.splitsOnOverfill_);
			TS_ASSERT_EQUALS(pageSize, stats.pageSize_);
			TS_ASSERT_EQUALS(2U, stats.numberOfBuckets_);
			TS_ASSERT_EQUALS(2 * numberOfRecords, stats.overflowFileDataPages_);
			TS_ASSERT_EQUALS(1U, stats.overflowFileBitmapPages_);

			TS_ASSERT_THROWS_NOTHING(db->close());
		}
	}
};

void DatabaseTest::testBucketSplitLargeValues()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestBucketSplitLargeValues(db, minPageDatabaseName, MIN_PAGE_SIZE));
	//TS_ASSERT_THROWS_NOTHING(doTestBucketSplitLargeValues(db, maxPageDatabaseName, MAX_PAGE_SIZE)); // This would take hours to finish.
}

//-----------------------------------------------------------------------------

namespace {

	template<class WriteBatchType, class ReadBatchType, class DeleteBatchType>
	void doTestBatchRequests(Database db, const std::string& name, size_type pageSize)
	{
		// Batch write 20 items.
		static const partNum_t INITAL_ELEMENTS = 20;
		static const partNum_t FIRST_DELETED_ELEMENT = 10;
		static const size_type VALUE_SIZE = 2048;

		WriteBatchType writeBatch;
		writeBatch.reserve(INITAL_ELEMENTS);
		for (partNum_t i = 0; i < INITAL_ELEMENTS; ++i) {
			const std::string key(keyFor(i));
			const std::string value = valueOfSize(VALUE_SIZE, i);

			writeBatch.add(key, i, value);
		}

		Options options = Options::readWriteSingleThreaded();
		options.pageSize_ = pageSize;

		TS_ASSERT_THROWS_NOTHING(db->open(name, options));
		TS_ASSERT_THROWS_NOTHING(db->store(writeBatch)); // Batch gets reordered for 64K pages.
		writeBatch.clear();

		// Check.
		for (partNum_t i = 0; i < INITAL_ELEMENTS; ++i) {
			const std::string key(keyFor(i));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, i, VALUE_SIZE, i));
		}

		RecordsIteratedOver allRecords(db);
		for (partNum_t i = 0; i < INITAL_ELEMENTS; ++i) {
			const std::string key(keyFor(i));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(allRecords, key, i, VALUE_SIZE, i));
		}
		TS_ASSERT(allRecords.empty());

		Statistics stats = db->statistics();
		TS_ASSERT_EQUALS(static_cast<uint64_t>(INITAL_ELEMENTS), stats.numberOfRecords_);

		// Batch read 20 items.
		ReadBatchType readAllBatch;
		readAllBatch.reserve(INITAL_ELEMENTS);
		for (partNum_t i = 0; i < INITAL_ELEMENTS; ++i) {
			const std::string key(keyFor(i));
			readAllBatch.add(key, i);
		}

		TS_ASSERT(db->fetch(readAllBatch));

		for (partNum_t i = 0; i < INITAL_ELEMENTS; ++i) {
			const std::string value = valueOfSize(VALUE_SIZE, i);
			TS_ASSERT_EQUALS(value, readAllBatch.resultAt(i));
		}

		// Batch delete last 10 items.
		DeleteBatchType deleteLastHalfBatch;
		for (partNum_t i = FIRST_DELETED_ELEMENT; i < INITAL_ELEMENTS; ++i) {
			const std::string key(keyFor(i));
			deleteLastHalfBatch.add(key, i);
		}

		TS_ASSERT_THROWS_NOTHING(db->remove(deleteLastHalfBatch));
		TS_ASSERT(! db->fetch(readAllBatch));

		// Check.
		for (partNum_t i = 0; i < FIRST_DELETED_ELEMENT; ++i) {
			const std::string key(keyFor(i));
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, i, VALUE_SIZE, i));
		}

		for (partNum_t i = FIRST_DELETED_ELEMENT; i < INITAL_ELEMENTS; ++i) {
			const std::string key(keyFor(i));
			TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, i));
		}

		RecordsIteratedOver remainingRecords(db);
		for (partNum_t i = 0; i < FIRST_DELETED_ELEMENT; ++i) {
			const std::string key(keyFor(i));
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(remainingRecords, key, i, VALUE_SIZE, i));
		}
		TS_ASSERT(remainingRecords.empty());


		// Batch read remaining 10 items.
		ReadBatchType readRemainingBatch;
		readAllBatch.reserve(FIRST_DELETED_ELEMENT);
		for (partNum_t i = 0; i < FIRST_DELETED_ELEMENT; ++i) {
			const std::string key(keyFor(i));
			readRemainingBatch.add(key, i);
		}

		TS_ASSERT(db->fetch(readRemainingBatch));

		for (partNum_t i = 0; i < FIRST_DELETED_ELEMENT; ++i) {
			const std::string value = valueOfSize(VALUE_SIZE, i);
			TS_ASSERT_EQUALS(value, readRemainingBatch.resultAt(i));
		}

		// Batch delete 10 remaining items.
		DeleteBatchType deleteFirstHalfBatch;
		for (partNum_t i = 0; i < FIRST_DELETED_ELEMENT; ++i) {
			const std::string key(keyFor(i));
			deleteFirstHalfBatch.add(key, i);
		}

		TS_ASSERT_THROWS_NOTHING(db->remove(deleteFirstHalfBatch));
		TS_ASSERT(! db->fetch(readAllBatch));
		TS_ASSERT(! db->fetch(readRemainingBatch));

		TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));
		TS_ASSERT_THROWS_NOTHING(db->close());
	}

}

void DatabaseTest::testReferenceBatchRequests()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING((doTestBatchRequests<StringWriteBatch, StringReadBatch, DeleteBatch>(db, minPageDatabaseName, MIN_PAGE_SIZE)));
	TS_ASSERT_THROWS_NOTHING((doTestBatchRequests<StringWriteBatch, StringReadBatch, DeleteBatch>(db, maxPageDatabaseName, MAX_PAGE_SIZE)));
}

void DatabaseTest::testCopyBatchRequests()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING((doTestBatchRequests<StringCopyWriteBatch, StringCopyReadBatch, DeleteBatch>(db, minPageDatabaseName, MIN_PAGE_SIZE)));
	TS_ASSERT_THROWS_NOTHING((doTestBatchRequests<StringCopyWriteBatch, StringCopyReadBatch, DeleteBatch>(db, maxPageDatabaseName, MAX_PAGE_SIZE)));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestBatchRequestDeleteAll(Database db, const std::string& name, size_type pageSize)
	{
		static const partNum_t ELEMENTS = 10;
		static const size_type VALUE_SIZE = 2048;

		const std::string key = "1";
		const std::string value = valueOfSize(VALUE_SIZE);

		StringWriteBatch writeBatch;
		for (partNum_t i = 0; i < ELEMENTS; ++i) {
			writeBatch.add(key, i, value);
		}

		// Batch write 10 items with the same key.
		Options options = Options::readWriteSingleThreaded();
		options.pageSize_ = pageSize;

		TS_ASSERT_THROWS_NOTHING(db->open(name, options));
		TS_ASSERT_THROWS_NOTHING(db->store(writeBatch));

		// Check.
		for (partNum_t i = 0; i < ELEMENTS; ++i) {
			TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, i, VALUE_SIZE));
		}

		RecordsIteratedOver records(db);
		for (partNum_t i = 0; i < ELEMENTS; ++i) {
			TS_ASSERT_THROWS_NOTHING(checkAndRemoveRecordOfSize(records, key, i, VALUE_SIZE));
		}
		TS_ASSERT(records.empty());

		Statistics stats = db->statistics();
		TS_ASSERT_EQUALS(static_cast<uint64_t>(ELEMENTS), stats.numberOfRecords_);

		// Batch delete key with ALL_PARTS.
		TS_ASSERT_THROWS_NOTHING(db->remove(key));
		TS_ASSERT_THROWS_NOTHING(checkDatabaseIsEmpty(db));

		TS_ASSERT_THROWS_NOTHING(db->close());
	}

};

void DatabaseTest::testBatchRequestDeleteAll()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestBatchRequestDeleteAll(db, minPageDatabaseName, MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestBatchRequestDeleteAll(db, maxPageDatabaseName, MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestStoreLimit(Database db, const std::string& name, size_type pageSize)
	{
		// Create a database, store limit is MAX_PAGE_SIZE + 2.
		const size_type STORE_LIMIT = MAX_PAGE_SIZE + 2;

		Options options = Options::readWriteSingleThreaded();
		options.storeThrowIfLargerThan_ = STORE_LIMIT;
		TS_ASSERT_THROWS_NOTHING(db->open(name, options));

		// Create a batch with 2 values smaller than limit and 2 values larger than limit.
		const std::string key = "1";

		StringWriteBatch writeBatch;
		writeBatch.add(key, 0, valueOfSize(STORE_LIMIT - 1));
		writeBatch.add(key, 1, valueOfSize(STORE_LIMIT));
		writeBatch.add(key, 2, valueOfSize(STORE_LIMIT + 1));
		writeBatch.add(key, 3, valueOfSize(STORE_LIMIT + 2));

		// Store must throw.
		TS_ASSERT_THROWS(db->store(writeBatch), ValueTooLarge);

		// Small values must be stored.
		TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, STORE_LIMIT - 1));
		TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, STORE_LIMIT));

		// Large values must not be stored.
		TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 2));
		TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 3));

		TS_ASSERT_THROWS_NOTHING(db->close());
	}

};

void DatabaseTest::testStoreLimit()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestStoreLimit(db, minPageDatabaseName, MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestStoreLimit(db, maxPageDatabaseName, MAX_PAGE_SIZE));
}

//-----------------------------------------------------------------------------

namespace {

	void doTestFetchLimit(Database db, const std::string& name, size_type pageSize)
	{
		// Create a database, fetch limit is MAX_PAGE_SIZE + 2.
		const size_type FETCH_LIMIT = MAX_PAGE_SIZE + 2;

		Options options = Options::readWriteSingleThreaded();
		options.fetchIgnoreIfLargerThan_ = FETCH_LIMIT;
		TS_ASSERT_THROWS_NOTHING(db->open(name, options));

		// Create a batch with 2 values smaller than limit and 2 values larger than limit.
		const std::string key = "1";

		StringWriteBatch writeBatch;
		writeBatch.add(key, 0, valueOfSize(FETCH_LIMIT - 1));
		writeBatch.add(key, 1, valueOfSize(FETCH_LIMIT));
		writeBatch.add(key, 2, valueOfSize(FETCH_LIMIT + 1));
		writeBatch.add(key, 3, valueOfSize(FETCH_LIMIT + 2));

		TS_ASSERT_THROWS_NOTHING(db->store(writeBatch));

		// Small values must be fetched.
		TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 0, FETCH_LIMIT - 1));
		TS_ASSERT_THROWS_NOTHING(checkRecordValueOfSize(db, key, 1, FETCH_LIMIT));

		// Large values must not be fetched.
		TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 2));
		TS_ASSERT_THROWS_NOTHING(checkRecordDoesNotExist(db, key, 3));

		TS_ASSERT_THROWS_NOTHING(db->close());
	}

};

void DatabaseTest::testFetchLimit()
{
	Database db = DatabaseFactory();

	const std::string minPageDatabaseName = databaseTestPath_ + "/dbMin";
	const std::string maxPageDatabaseName = databaseTestPath_ + "/dbMax";

	TS_ASSERT_THROWS_NOTHING(doTestFetchLimit(db, minPageDatabaseName, MIN_PAGE_SIZE));
	TS_ASSERT_THROWS_NOTHING(doTestFetchLimit(db, maxPageDatabaseName, MAX_PAGE_SIZE));
}
