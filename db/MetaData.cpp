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

// MetaData.cpp - keeps metadata for database page allocation/deallocation.
#include "stdafx.h"
#include "OpenFiles.h"
#include "MetaData.h"
#include "BitmapPage.h"

namespace kerio {
namespace hashdb {

	namespace {

		uint32_t computeMaskFrom(const uint32_t highestBucket)
		{
			register uint32_t mask = highestBucket + 1;

			mask |= (mask >> 1);
			mask |= (mask >> 2);
			mask |= (mask >> 4);
			mask |= (mask >> 8);
			mask |= (mask >> 16);

			return mask;
		}

		uint32_t computeBucketToSplit(const uint32_t highestBucket, const uint32_t highMask)
		{
			return (highestBucket + 1) & (highMask >> 1);
		}

	};


	//----------------------------------------------------------------------------
	// Ctor.

	MetaData::MetaData(Environment& environment, OpenFiles& openFiles, const Options& options)
		: highestBucket_(openFiles.bucketHeaderPage()->getHighestBucket())
		, highMask_(computeMaskFrom(highestBucket_))
		, bucketToSplit_(computeBucketToSplit(highestBucket_, highMask_))
		, numberOfRecords_(openFiles.bucketHeaderPage()->getDatabaseNumberOfRecords())
		, dataInlineSize_(openFiles.bucketHeaderPage()->getDataSize())
		, leavePageFreeSpace_(options.leavePageFreeSpace_)
		, overflowFileManager_(environment, openFiles)
		, bucketPagesAcquired_(0)
		, overflowPagesAcquired_(0)
		, largeValuePagesAcquired_(0)
		, bucketPagesReleased_(0)
		, overflowPagesReleased_(0)
		, largeValuePagesReleased_(0)
		, splitsOnOverfill_(0)
		, hashFun_(options.hashFun_)
		, openFiles_(openFiles)
		, unsavedChanges_(0)
		, minFlushFrequency_(options.minFlushFrequency_)
	{

	}
	
	//----------------------------------------------------------------------------
	// Saving to header pages.

	void MetaData::save(bool forceSave /* = false*/ )
	{
		if ((forceSave && unsavedChanges_ != 0) || (unsavedChanges_ >= minFlushFrequency_)) {

			HeaderPage* bucketHeaderPage = openFiles_.bucketHeaderPage();

			bucketHeaderPage->setHighestPageNumber(highestBucket_ + 1);
			bucketHeaderPage->setHighestBucket(highestBucket_);
			bucketHeaderPage->setDatabaseNumberOfRecords(numberOfRecords_);
			bucketHeaderPage->setDataSize(dataInlineSize_);

			// Write also to overflow header to aid recovery.
			HeaderPage* overflowHeaderPage = openFiles_.overflowHeaderPage();

			overflowHeaderPage->setHighestPageNumber(overflowFileManager_.highestOverflowFilePage());
			overflowHeaderPage->setHighestBucket(highestBucket_);
			overflowHeaderPage->setDatabaseNumberOfRecords(numberOfRecords_);
			overflowHeaderPage->setDataSize(dataInlineSize_);
		
			openFiles_.saveHeaderPages();
			overflowFileManager_.save();
			unsavedChanges_ = 0;
		}
	}
	
    //----------------------------------------------------------------------------
	// Management of the bucket file.

	uint32_t MetaData::newBucketNumber()
	{
		const uint32_t newBucketNumber = highestBucket_ + 1;
		RAISE_INVALID_ARGUMENT_IF(newBucketNumber == std::numeric_limits<uint32_t>::max(), "database is full: buckets exhausted");
		highestBucket_ = newBucketNumber;

		highMask_ = computeMaskFrom(highestBucket_);
		bucketToSplit_ = computeBucketToSplit(highestBucket_, highMask_);

		++bucketPagesAcquired_;
		increaseSaveImportance();

		return newBucketNumber;
	}

	uint32_t MetaData::bucketForKey(const boost::string_ref& key) const
	{
		const uint32_t hash = hashFun_(key.data(), key.size());
		const uint32_t bucket = hash & highMask_;

		return (bucket > highestBucket_)? (bucket & (highMask_ >> 1)) : bucket;
	}

	uint32_t MetaData::highestBucket() const
	{
		return highestBucket_;
	}

	uint32_t MetaData::bucketToSplit() const
	{
		return bucketToSplit_;
	}
	
    //----------------------------------------------------------------------------
	// Management of the overflow file.

	uint32_t MetaData::acquireOverflowPageNumber()
	{
		const uint32_t pageNumber = doAcquireOverflowFilePageNumber();
		++overflowPagesAcquired_;
		return pageNumber;
	}

	void MetaData::releaseOverflowPageNumber(uint32_t pageNumber)
	{
		doReleaseOverflowFilePageNumber(pageNumber);
		++overflowPagesReleased_;
	}

	uint32_t MetaData::acquireLargeValuePageNumber()
	{
		const uint32_t pageNumber = doAcquireOverflowFilePageNumber();
		++largeValuePagesAcquired_;
		return pageNumber;
	}

	void MetaData::releaseLargeValuePageNumber(uint32_t pageNumber)
	{
		doReleaseOverflowFilePageNumber(pageNumber);
		++largeValuePagesReleased_;
	}

	uint32_t MetaData::doAcquireOverflowFilePageNumber()
	{
		return overflowFileManager_.acquireOverflowPageNumber();
	}

	void MetaData::doReleaseOverflowFilePageNumber(uint32_t pageNumber)
	{
		overflowFileManager_.releaseOverflowPageNumber(pageNumber);
	}

    //----------------------------------------------------------------------------
    // Statistics.

	void MetaData::recordAdded(size_type recordInlineSize)
	{
		const uint64_t newNumberOfRecords = numberOfRecords_ + 1;
		RAISE_INVALID_ARGUMENT_IF(newNumberOfRecords == 0, "database is full: record numbers exhausted");

		const uint64_t newDataSize = dataInlineSize_ + recordInlineSize;
		RAISE_INVALID_ARGUMENT_IF(newDataSize < dataInlineSize_, "database is full: inline data size exhausted");

		numberOfRecords_ = newNumberOfRecords;
		dataInlineSize_ = newDataSize;

		increaseSaveImportance();
	}

	void MetaData::recordRemoved(size_type recordInlineSize)
	{
		RAISE_DATABASE_CORRUPTED_IF(numberOfRecords_ == 0, "number of records is 0 while deleting a record");
		RAISE_DATABASE_CORRUPTED_IF(dataInlineSize_ < recordInlineSize, "inline data size is smaller than inline record size while deleting a record");

		--numberOfRecords_;
		dataInlineSize_ -= recordInlineSize;

		increaseSaveImportance();
	}

	void MetaData::incrementOverfillStatistics()
	{
		++splitsOnOverfill_;
	}

	kerio::hashdb::size_type MetaData::actualFill(size_type recordInlineSize) const
	{
		const size_type buckets = highestBucket_ + 1;
		const size_type averageDataPerPage = static_cast<size_type>((dataInlineSize_ + recordInlineSize) / buckets);
		const size_type averagePointersPerPage = static_cast<size_type>((numberOfRecords_ + 1) / buckets);

		return averageDataPerPage + (averagePointersPerPage * sizeof(uint16_t));
	}

	kerio::hashdb::size_type MetaData::expectedFill() const
	{
		return DataPage::dataSpace(openFiles_.pageSize()) - static_cast<size_type>(leavePageFreeSpace_);
	}

	bool MetaData::isOverfill(size_type recordInlineSize) const
	{
		// The Seltzer - Yigit article article suggests that the best time to initiate a bucket split is when
		// the space occupied by inline data exceeds the average of a single page per bucket. We let the user 
		// to increase or decrease the amount with the leavePageFreeSpace_ option.

		const size_type computedActualFill = actualFill(recordInlineSize);
		const size_type computedExpectedFill = expectedFill();

		return computedActualFill > computedExpectedFill;
	}

	Statistics MetaData::statistics() const
	{
		Statistics stats;

		stats.bucketPagesAcquired_ = bucketPagesAcquired_;
		stats.overflowPagesAcquired_ = overflowPagesAcquired_;
		stats.largeValuePagesAcquired_ = largeValuePagesAcquired_;
		stats.bitmapPagesAcquired_ = overflowFileManager_.bitmapPagesAcquired();

		stats.bucketPagesReleased_ = bucketPagesReleased_;
		stats.overflowPagesReleased_ = overflowPagesReleased_;
		stats.largeValuePagesReleased_ = largeValuePagesReleased_;
		stats.bitmapPagesReleased_ = overflowFileManager_.bitmapPagesReleased();

		stats.splitsOnOverfill_ = splitsOnOverfill_;
		stats.cachedPages_ = overflowFileManager_.heldPages();

		stats.pageSize_ = openFiles_.pageSize();
		stats.numberOfBuckets_ = highestBucket_ + 1;
		stats.overflowFileDataPages_ = overflowFileManager_.overflowFileDataPages();
		stats.overflowFileBitmapPages_ = overflowFileManager_.overflowFileBitmapPages();
		stats.numberOfRecords_ =  numberOfRecords_;
		stats.dataInlineSize_ = dataInlineSize_;

		return stats;
	}

	void MetaData::increaseSaveImportance()
	{
		++unsavedChanges_;
	}

}; // namespace hashdb
}; // namespace kerio
