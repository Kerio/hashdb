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

// OpenDatabase.cpp - represents an open database.
#include "stdafx.h"
#include <limits>
#include <iostream>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <kerio/hashdb/StringOrReference.h>
#include "BucketDataPage.h"
#include "OverflowDataPage.h"
#include "LargeValuePage.h"
#include "DataPageCursor.h"
#include "OpenDatabase.h"

namespace kerio {
namespace hashdb {

	//-------------------------------------------------------------------------
	// Utilities.

	template<typename T>
	class PageLoader
	{
	public:
		PageLoader(Environment& environment, OpenFiles& openFiles)
			: openFiles_(openFiles)
			, page_(environment.pageAllocator(), openFiles.pageSize())
		{

		}

		~PageLoader()
		{
			openFiles_.write(page_);
		}

		T& page(const PageId& pageId)
		{
			if (page_.getId() != pageId) {
				save();
				load(pageId);
				page_.validate();
			}

			return page_;
		}

		T& newPage(const uint32_t pageNumber)
		{
			if (page_.getId().pageNumber() != pageNumber) {
				save();
			}

			page_.setUp(pageNumber);
			return page_;
		}

		void invalidate()
		{
			page_.disown();
		}

		void save()
		{
			if (page_.getId().isValid()) {
				openFiles_.write(page_);
			}
		}

	private:
		void load(const PageId& pageId)
		{
			openFiles_.read(page_, pageId);
		}

		OpenFiles& openFiles_;
		T page_;
	};

	// This initial implementation of SingleRequestCache holds only a single instance of bucket page and a single instance of overflow page.
	// A reference to the instance is valid only until the next call to the cache.
	class SingleRequestCache {
	public:
		SingleRequestCache(Environment& environment, OpenFiles& openFiles)
			: bucketPageLoader_(environment, openFiles)
			, overflowPageLoader_(environment, openFiles)
		{ }

		DataPage& dataPage(const PageId& pageId)
		{
			if (pageId.fileType() == PageId::BucketFileType) {
				return bucketPageLoader_.page(pageId);
			}
			else {
				return overflowPageLoader_.page(pageId);
			}
		}

		OverflowDataPage& newOverflowPage(uint32_t newPageNumber)
		{
			OverflowDataPage& newPage = overflowPageLoader_.newPage(newPageNumber);
			return newPage;
		}

		void save()
		{
			bucketPageLoader_.save();
			overflowPageLoader_.save();
		}

		void invalidate()
		{
			bucketPageLoader_.invalidate();
			overflowPageLoader_.invalidate();
		}

	private:
		PageLoader<BucketDataPage> bucketPageLoader_;
		PageLoader<OverflowDataPage> overflowPageLoader_;
	};

	//-------------------------------------------------------------------------
	// Access order for read/write/delete batches.

	class BatchReorderItem {
	public:

		BatchReorderItem(size_t index, uint32_t bucketNumber)
			: index_(index)
			, bucketNumber_(bucketNumber)
		{ }

		size_t index() const
		{
			return index_;
		}

		bool operator<(const BatchReorderItem& right) const
		{
			return bucketNumber_ < right.bucketNumber_;
		}

	private:
		size_t index_;
		uint32_t bucketNumber_;
	};

	typedef Vector<BatchReorderItem, OpenDatabase::ASSUMED_BATCH_SIZE_MAX_SIZE> batchAccessVector_t;

	void createBatchAccessOrder(batchAccessVector_t& accessOrder, const MetaData& metaData, const IKeyProducer& batch)
	{
		const size_type batchSize = static_cast<size_type>(batch.count());
		accessOrder.reserve(batchSize);

		for (size_type i = 0; i < batchSize; ++i) {
			const StringOrReference	keyHolder = batch.keyAt(i);
			const uint32_t bucket = metaData.bucketForKey(keyHolder.getRef());
			accessOrder.emplace_back(i, bucket);
		}

		accessOrder.sort();
	}

	//-------------------------------------------------------------------------
	// Creation and destruction.

	OpenDatabase::OpenDatabase(const boost::filesystem::path& database, const Options& options)
		: environment_(options)
		, openFiles_(database, options, environment_)
		, metaData_(environment_, openFiles_, options)
		, storeThrowIfLargerThan_(options.storeThrowIfLargerThan_)
		, fetchIgnoreIfLargerThan_(options.fetchIgnoreIfLargerThan_)
	{
		if (openFiles_.isNew()) {
			size_type bucketsToCreate = options.initialBuckets_;

			HASHDB_LOG_DEBUG("Creating %u empty buckets", bucketsToCreate);
			createInitialBucketPages(bucketsToCreate);
		}
		else {
			HASHDB_LOG_DEBUG("Number of buckets in database is %u", metaData_.highestBucket() + 1);
		}
	}

	void OpenDatabase::createInitialBucketPages(const size_type initialBuckets)
	{
		BucketDataPage bucketPage(environment_.pageAllocator(), openFiles_.pageSize());

		// Create initial page + additional pages.
		for (size_type i = 0; i < initialBuckets; ++i) {
			uint32_t pageNumber = metaData_.newBucketNumber() + 1;
			bucketPage.setUp(pageNumber);
			openFiles_.write(bucketPage);
		}

		flush();
	}

	void OpenDatabase::close()
	{
		flush();
		openFiles_.close();
	}

	bool OpenDatabase::isClosed() const
	{
		return openFiles_.isClosed();
	}

	OpenDatabase::~OpenDatabase()
	{
		HASHDB_ASSERT(isClosed());

		if (! isClosed()) {
			try {
				close();
			} catch (std::exception&) {
				// Ignore.
			}
		}
	}

	//-------------------------------------------------------------------------
	// Saving buffers.

	void OpenDatabase::flush()
	{
		metaData_.save(true);
	}

	void OpenDatabase::sync()
	{
		flush();
		openFiles_.sync();
	}

	//-------------------------------------------------------------------------
	// Releasing unnecessary resources.

	void OpenDatabase::releaseSomeResources()
	{
		environment_.pageAllocator()->freeSomeMemory();
	}

	//-------------------------------------------------------------------------
	// Prefetching database pages.

	void OpenDatabase::prefetch()
	{
		openFiles_.prefetch();
	}

	//-------------------------------------------------------------------------
	// Listing all parts.

	std::vector<partNum_t> OpenDatabase::listParts(const boost::string_ref& key)
	{
		const uint32_t bucketNumber = metaData_.bucketForKey(key);
		PageId pageId(bucketFilePage(bucketNumber + 1));

		SingleRequestCache cache(environment_, openFiles_);
		std::vector<partNum_t> rv;
		size_type numberOfTraversedPages = 0;

		while (pageId.isValid()) {
			DataPage& page = cache.dataPage(pageId);

			DataPageCursor cursor(&page);
			while (cursor.find(key)) {
				rv.push_back(cursor.partNum());
				cursor.next();
			}

			pageId = page.nextOverflowPageId();
			incrementTraversedPages(numberOfTraversedPages, pageId);
		}

		std::sort(rv.begin(), rv.end());
		return rv;
	}

	//-------------------------------------------------------------------------
	// Reading from the database.

	void OpenDatabase::fetchLargeValue(std::string& outValue, const size_type valueSize, const PageId& firstLargeValuePageId)
	{
		if (! outValue.empty()) {
			outValue.clear();
		}
		
		if (outValue.capacity() < valueSize) {
			outValue.reserve(valueSize); // May raise std::bad_alloc.
		}

		LargeValuePage largeValuePage(environment_.pageAllocator(), openFiles_.pageSize());
		PageId largeValuePageId = firstLargeValuePageId;

		bool fetchNextPart = true;
		while (fetchNextPart) {
			RAISE_DATABASE_CORRUPTED_IF(! largeValuePageId.isValid(), "actual large value size is smaller than %u recorded in metadata", valueSize);

			openFiles_.read(largeValuePage, largeValuePageId);
			fetchNextPart = largeValuePage.getValuePart(outValue, valueSize);
			largeValuePageId = largeValuePage.nextLargeValuePageId();
		}

		RAISE_DATABASE_CORRUPTED_IF(largeValuePageId.isValid(), "actual large value size is greater than %u recorded in metadata", valueSize);
	}

	bool OpenDatabase::fetchSingleValueAt(SingleRequestCache& cache, IReadBatch& readBatch, size_t index)
	{
		const StringOrReference keyHolder = readBatch.keyAt(index);
		const boost::string_ref key = keyHolder.getRef();
		
		const partNum_t partNum = readBatch.partNumAt(index);
		const RecordId recordId(key, partNum);

		const uint32_t bucketNumber = metaData_.bucketForKey(key);
		PageId pageId(bucketFilePage(bucketNumber + 1));

		bool success = false;
		while (! success && pageId.isValid()) {
			DataPage& page = cache.dataPage(pageId);

			DataPageCursor cursor(&page);
			if (cursor.find(recordId)) {
				
				if (cursor.isInlineValue()) {
					success = readBatch.setValueAt(index, cursor.inlineValue());
					break;
				}
				else {
					std::string fetchedValue;
					const size_type valueSize = cursor.largeValueSize();

					if (fetchIgnoreIfLargerThan_ != 0 && valueSize > fetchIgnoreIfLargerThan_) {
						break;
					}

					// TODO: fetch the value on demand rather than at once.
					fetchLargeValue(fetchedValue, valueSize, cursor.firstLargeValuePageId());

					typedef boost::iostreams::basic_array_source<char> Device;
					boost::iostreams::stream<Device> stream(fetchedValue.data(), fetchedValue.size());

					success = readBatch.setLargeValueAt(index, stream, valueSize);
					break;
				}
			}

			pageId = page.nextOverflowPageId();
		}

		return success;
	}

	bool OpenDatabase::fetch(IReadBatch& readBatchRef)
	{
		SingleRequestCache cache(environment_, openFiles_);

		const size_type batchSize = static_cast<size_type>(readBatchRef.count());
		size_type valuesFoundAndSet = 0;

		if (batchSize < MIN_BATCH_SIZE_TO_REORDER) {

			for (size_type i = 0; i < batchSize; ++i) {
				if (fetchSingleValueAt(cache, readBatchRef, i)) {
					++valuesFoundAndSet;
				}
			}

		}
		else {
			batchAccessVector_t accessOrder;
			createBatchAccessOrder(accessOrder, metaData_, readBatchRef);

			for (batchAccessVector_t::iterator ii = accessOrder.begin(); ii != accessOrder.end(); ++ii) {
				const size_t accessIndex = ii->index();

				if (fetchSingleValueAt(cache, readBatchRef, accessIndex)) {
					++valuesFoundAndSet;
				}
			}
		}

		return batchSize == valuesFoundAndSet;
	}

	//-------------------------------------------------------------------------
	// Deleting from the database.

	void OpenDatabase::freeLargeValuePages(const PageId& firstLargeValuePageId, const size_type valueSize)
	{
		size_type remainingValueSize = valueSize;

		LargeValuePage largeValuePage(environment_.pageAllocator(), openFiles_.pageSize());
		PageId largeValuePageId = firstLargeValuePageId;

		while (remainingValueSize != 0) {
			RAISE_DATABASE_CORRUPTED_IF(! largeValuePageId.isValid(), "actual large value size is smaller than %u recorded in metadata", valueSize);

			openFiles_.read(largeValuePage, largeValuePageId);
			RAISE_DATABASE_CORRUPTED_IF(largeValuePage.getMagic() != largeValuePage.magic(), "bad large page value magic number %08x on %s", largeValuePage.getMagic(), largeValuePageId.toString());
			metaData_.releaseLargeValuePageNumber(largeValuePageId.pageNumber());

			remainingValueSize -= largeValuePage.partSize(remainingValueSize);
			largeValuePageId = largeValuePage.nextLargeValuePageId();
		}
		
		RAISE_DATABASE_CORRUPTED_IF(largeValuePageId.isValid(), "actual large value size is greater than %u recorded in metadata", valueSize);
	}

	void OpenDatabase::removeRecord(DataPage& page, DataPageCursor cursor)
	{
		if (! cursor.isInlineValue()) {
			freeLargeValuePages(cursor.firstLargeValuePageId(), cursor.largeValueSize());
		}

		const size_type removedRecordInlineSize = page.deleteSingleRecord(cursor);
		metaData_.recordRemoved(removedRecordInlineSize);
	}

	void OpenDatabase::removeSingleValue(SingleRequestCache& cache, const boost::string_ref& key, partNum_t partNum)
	{
		const RecordId recordId(key, partNum);
		
		const uint32_t bucketNumber = metaData_.bucketForKey(key);
		PageId currentPageId(bucketFilePage(bucketNumber + 1));
		size_type numberOfTraversedPages = 0;

		while (currentPageId.isValid()) {
			DataPage& page = cache.dataPage(currentPageId);

			DataPageCursor cursor(&page);
			if (cursor.find(recordId)) {
				removeRecord(page, cursor);
				break;
			}

			currentPageId = page.nextOverflowPageId();
			incrementTraversedPages(numberOfTraversedPages, currentPageId);
		}
	}

	void OpenDatabase::removeAllParts(SingleRequestCache& cache, const boost::string_ref& key)
	{
		const uint32_t bucketNumber = metaData_.bucketForKey(key);
		PageId pageId(bucketFilePage(bucketNumber + 1));
		size_type numberOfTraversedPages = 0;

		while (pageId.isValid()) {
			DataPage& page = cache.dataPage(pageId);

			DataPageCursor cursor(&page);
			while (cursor.find(key)) {
				removeRecord(page, cursor);
				cursor.reset();
			}

			pageId = page.nextOverflowPageId();
			incrementTraversedPages(numberOfTraversedPages, pageId);
		}
	}

	void OpenDatabase::remove(const IDeleteBatch& deleteBatch)
	{
		SingleRequestCache cache(environment_, openFiles_);

		const size_type batchSize = static_cast<size_type>(deleteBatch.count());

		if (batchSize < MIN_BATCH_SIZE_TO_REORDER) {

			for (size_t i = 0; i < batchSize; ++i) {
				const StringOrReference keyHolder = deleteBatch.keyAt(i);
				const partNum_t partNum = deleteBatch.partNumAt(i);

				if (partNum == ALL_PARTS) {
					removeAllParts(cache, keyHolder.getRef());
				}
				else {
					removeSingleValue(cache, keyHolder.getRef(), partNum);
				}
			}

		}
		else {
			batchAccessVector_t accessOrder;
			createBatchAccessOrder(accessOrder, metaData_, deleteBatch);

			for (batchAccessVector_t::iterator ii = accessOrder.begin(); ii != accessOrder.end(); ++ii) {
				const size_t accessIndex = ii->index();

				const StringOrReference keyHolder = deleteBatch.keyAt(accessIndex);
				const partNum_t partNum = deleteBatch.partNumAt(accessIndex);

				if (partNum == ALL_PARTS) {
					removeAllParts(cache, keyHolder.getRef());
				}
				else {
					removeSingleValue(cache, keyHolder.getRef(), partNum);
				}
			}

		}

		metaData_.save();
	}

	//-------------------------------------------------------------------------
	// Writing to the database.

	class SplitPages {
	public:

		SplitPages(uint32_t bucketNumber, IPageAllocator* allocator, size_type pageSize)
			: bucketNumber_(bucketNumber)
			, allocator_(allocator)
			, pageSize_(pageSize)
			, bucketPage_(allocator, pageSize)
			, records_(0)
		{
			bucketPage_.setUp(bucketNumber + 1);
		}

		uint32_t bucket()
		{
			return bucketNumber_;
		}

		size_type addRecord(const DataPageCursor& cursor)
		{
			const boost::string_ref recordInlineData = cursor.inlineRecord();
			size_type addedSize = lastPage().addSingleRecord(recordInlineData);

			if (addedSize == 0) {
				addedSize = newPage().addSingleRecord(recordInlineData);
				RAISE_INTERNAL_ERROR_IF(addedSize == 0, "unable to add overflow record on page split");
			}

			++records_;
			return addedSize;
		}

		size_type addRecord(const RecordId& recordId, const DataPage::AddedValueRef& valueRef)
		{
			size_type addedSize = lastPage().addSingleRecord(recordId, valueRef);

			if (addedSize == 0) {
				addedSize = newPage().addSingleRecord(recordId, valueRef);
				RAISE_INTERNAL_ERROR_IF(addedSize == 0, "unable to add new record on page split");
			}

			++records_;
			return addedSize;
		}

		void write(MetaData& metaData, OpenFiles& openFiles)
		{
			if (! overflowChain_.empty()) {
				const size_type chainSize = overflowChain_.size();

				uint32_t overflowPageNumber = metaData.acquireOverflowPageNumber();
				bucketPage_.setNextOverflowPage(overflowPageNumber);

				for (size_type i = 0; i < chainSize; ++i) {
					OverflowDataPage& page = overflowChain_[i];
					page.setPageNumber(overflowPageNumber);
					page.setId(overflowFilePage(overflowPageNumber));

					if (i + 1 < chainSize) {
						overflowPageNumber = metaData.acquireOverflowPageNumber();
						page.setNextOverflowPage(overflowPageNumber);
					}

					openFiles.write(page);
				}
			}

			// Bucket page is overwritten last.
			openFiles.write(bucketPage_);
		}

		size_type records()
		{
			return records_;
		}

	private:
		DataPage& lastPage()
		{
			return (overflowChain_.empty())? static_cast<DataPage&>(bucketPage_) : static_cast<DataPage&>(overflowChain_.back());
		}

		DataPage& newPage()
		{
			OverflowDataPage& page = overflowChain_.emplace_back(allocator_, pageSize_);
			page.setUp(0);
			return page;
		}

		const uint32_t bucketNumber_;

		IPageAllocator* allocator_;
		const size_type pageSize_;

		BucketDataPage bucketPage_;

		typedef Vector<OverflowDataPage, OpenDatabase::ASSUMED_OVERFLOW_CHAIN_MAX_SIZE> OverflowChain_t;
		OverflowChain_t overflowChain_;

		size_type records_;
	};

	void OpenDatabase::splitToChains(SingleRequestCache& cache, OriginalOverflowPageNumbers_t& originalOverflowPageNumbers, SplitPages& chainBeingSplit, SplitPages& newChain)
	{
		// Read the original chain and split it to two chains.
		PageId pageId(bucketFilePage(chainBeingSplit.bucket() + 1));
		while (pageId.isValid()) {

			if (pageId.fileType() == PageId::OverflowFileType) {
				originalOverflowPageNumbers.push_back(pageId.pageNumber());
				RAISE_DATABASE_CORRUPTED_IF(originalOverflowPageNumbers.size() > ALLOWED_OVERFLOW_CHAIN_MAX_SIZE, "cycle in overflow page chain (done %u page traversals) on %s", originalOverflowPageNumbers.size(), pageId.toString());
			}

			DataPage& page = cache.dataPage(pageId);
			DataPageCursor cursor(&page);

			while (cursor.isValid()) {
				const boost::string_ref key = cursor.key();
				const uint32_t bucket = metaData_.bucketForKey(key);
				
				HASHDB_LOG_DEBUG_DETAIL("Split of bucket %u: record key=\"%s\" (%s) moved to bucket %u", chainBeingSplit.bucket(), cursor.key(), pageId.toString(), bucket);

				if (bucket == chainBeingSplit.bucket()) {
					chainBeingSplit.addRecord(cursor);
				}
				else if (bucket == newChain.bucket()) {
					newChain.addRecord(cursor);
				}
				else {
					RAISE_INTERNAL_ERROR("invalid bucket number %u, expected %u or %u when splitting on %s", bucket, chainBeingSplit.bucket(), newChain.bucket(), pageId.toString());
				}

				cursor.next();
			}

			pageId = page.nextOverflowPageId();
		}

		// Get rid of the cache.
		cache.save();
		cache.invalidate();

		originalOverflowPageNumbers.sort();
	}

	void OpenDatabase::splitOnOverfill(SingleRequestCache& cache)
	{
		const uint32_t bucketToSplitNumber = metaData_.bucketToSplit();
		const uint32_t newBucketNumber = metaData_.newBucketNumber();

		SplitPages chainBeingSplit(bucketToSplitNumber, environment_.pageAllocator(), openFiles_.pageSize());
		SplitPages newChain(newBucketNumber, environment_.pageAllocator(), openFiles_.pageSize());
		OriginalOverflowPageNumbers_t originalOverflowPageNumbers;

		splitToChains(cache, originalOverflowPageNumbers, chainBeingSplit, newChain);

		// Write the changes.
		chainBeingSplit.write(metaData_, openFiles_);
		newChain.write(metaData_, openFiles_);

		// Release original overflow pages.
		for (OriginalOverflowPageNumbers_t::iterator ii = originalOverflowPageNumbers.begin(); ii != originalOverflowPageNumbers.end(); ++ii) {
			metaData_.releaseOverflowPageNumber(*ii);
		}

		HASHDB_LOG_DEBUG("Split bucket %u with %u overflow pages and %u records: %u records remained in bucket %u, %u records moved to new bucket %u",
			bucketToSplitNumber, originalOverflowPageNumbers.size(), chainBeingSplit.records() + newChain.records(),
			chainBeingSplit.records(), chainBeingSplit.bucket(), newChain.records(), newChain.bucket());
	}

	size_type OpenDatabase::splitAddRecordOnOverflow(SingleRequestCache& cache, const RecordId& recordId, const DataPage::AddedValueRef& valueRef)
	{
		const uint32_t bucketToSplitNumber = metaData_.bucketToSplit();
		const uint32_t newBucketNumber = metaData_.newBucketNumber();

		SplitPages chainBeingSplit(bucketToSplitNumber, environment_.pageAllocator(), openFiles_.pageSize());
		SplitPages newChain(newBucketNumber, environment_.pageAllocator(), openFiles_.pageSize());
		OriginalOverflowPageNumbers_t originalOverflowPageNumbers;

		splitToChains(cache, originalOverflowPageNumbers, chainBeingSplit, newChain);

		// Add new record to the end of the appropriate chain.
		const uint32_t newRecordBucket = metaData_.bucketForKey(recordId.key());
		HASHDB_LOG_DEBUG_DETAIL("Split of bucket %u: new record key=\"%s\" added to bucket %u", bucketToSplitNumber, recordId.key().to_string(), newRecordBucket);

		size_type addedSize = 0;
		if (newRecordBucket == bucketToSplitNumber) {
			addedSize = chainBeingSplit.addRecord(recordId, valueRef);
		}
		else if (newRecordBucket == newBucketNumber) {
			addedSize = newChain.addRecord(recordId, valueRef);
		}
		else {
			RAISE_INTERNAL_ERROR("invalid bucket number %u, expected %u or %u when adding new record after split", newRecordBucket, bucketToSplitNumber, newBucketNumber);
		}

		// Write the changes.
		chainBeingSplit.write(metaData_, openFiles_);
		newChain.write(metaData_, openFiles_);

		// Release original overflow pages.
		for (OriginalOverflowPageNumbers_t::iterator ii = originalOverflowPageNumbers.begin(); ii != originalOverflowPageNumbers.end(); ++ii) {
			metaData_.releaseOverflowPageNumber(*ii);
		}

		HASHDB_LOG_DEBUG("Split bucket %u with %u overflow pages and %u records: %u records remained in bucket %u, %u records moved to new bucket %u, 1 new record added to bucket %u",
			bucketToSplitNumber, originalOverflowPageNumbers.size(), chainBeingSplit.records() + newChain.records(),
			chainBeingSplit.records(), chainBeingSplit.bucket(), newChain.records(), newChain.bucket(), newRecordBucket);

		return addedSize;
	}

	void OpenDatabase::storeSingleValue(SingleRequestCache& cache, const boost::string_ref& key, partNum_t partNum, const boost::string_ref& value)
	{
		const RecordId recordId(key, partNum);

		const uint32_t bucketNumberForKey = metaData_.bucketForKey(key);
		const PageId bucketPageId(bucketFilePage(bucketNumberForKey + 1));
		bool skipInsert = false;

		PageId currentPageId(bucketPageId);

		// Delete existing record if any.
		size_type pagesTraversersedWhenDeleting = 0;
		while (currentPageId.isValid()) {
			DataPage& delDataPage = cache.dataPage(currentPageId);

			DataPageCursor cursor(&delDataPage);
			if (cursor.find(recordId)) {
				if (cursor.isInlineValue() && value == cursor.inlineValue()) {
						skipInsert = true;
				}
				else {
					removeRecord(delDataPage, cursor);
					HASHDB_LOG_DEBUG_DETAIL("Removed old record key=\"%s\" from bucket %u (%s)", key.to_string(), bucketNumberForKey, currentPageId.toString());
				}

				break;
			}

			currentPageId = delDataPage.nextOverflowPageId();
			incrementTraversedPages(pagesTraversersedWhenDeleting, currentPageId);
		}

		// Add new value.
		if (! skipInsert) {
			const size_type recordOverheadSize = recordId.recordOverheadSize();
			const bool isInlineRecord = (recordOverheadSize + value.size()) <= cache.dataPage(bucketPageId).largestPossibleInlineRecordSize();

			// If value is a large value, split it to large value pages.
			PageId firstLargeValuePageId;
			if (! isInlineRecord) {
				LargeValuePage largeValuePage(environment_.pageAllocator(), openFiles_.pageSize());

				size_type putPosition = 0;
				bool valuePartRemains;

				uint32_t newLargeValuePageNumber = metaData_.acquireLargeValuePageNumber();
				firstLargeValuePageId = PageId(overflowFilePage(newLargeValuePageNumber));

				do {
					largeValuePage.setUp(newLargeValuePageNumber);
					valuePartRemains = largeValuePage.putValuePart(putPosition, value);

					if (valuePartRemains) {
						newLargeValuePageNumber = metaData_.acquireLargeValuePageNumber();
						largeValuePage.setNextLargeValuePage(newLargeValuePageNumber);
					}

					openFiles_.write(largeValuePage);
				} while (valuePartRemains);
			}

			// Create reference to value (for inline value) or to value size + id of first large value page (for a large value).
			DataPage::AddedValueRef addedValueRef = isInlineRecord?
				DataPage::AddedValueRef(value) : DataPage::AddedValueRef(static_cast<size_type>(value.size()), firstLargeValuePageId);

			// Add to an existing bucket/overflow page if possible.
			currentPageId = bucketPageId;
			PageId parentPageId;
			size_type addedInlineRecordSize = 0;
			size_type pagesTraversersedWhenInserting = 0;

			while (currentPageId.isValid()) {
				parentPageId = currentPageId;
				DataPage& addDataPage = cache.dataPage(currentPageId);
				
				addedInlineRecordSize = addDataPage.addSingleRecord(recordId, addedValueRef);
				if (addedInlineRecordSize != 0) {
					HASHDB_LOG_DEBUG_DETAIL("Added new %s record key=\"%s\" to bucket %u (%s)", (isInlineRecord)? "inline" : "large", key.to_string(), bucketNumberForKey, currentPageId.toString());
					break;
				}

				currentPageId = addDataPage.nextOverflowPageId();
				incrementTraversedPages(pagesTraversersedWhenInserting, currentPageId);
			}

			// Record could not be added to an existing page.
			if (addedInlineRecordSize == 0) {

				// Split on overflow? (Aka "uncontrolled split".)
				if (bucketNumberForKey == metaData_.bucketToSplit()) {
					HASHDB_LOG_DEBUG_DETAIL("Overflow detected in bucket %u, traversed %u pages", bucketNumberForKey, pagesTraversersedWhenInserting);
					addedInlineRecordSize = splitAddRecordOnOverflow(cache, recordId, addedValueRef);
				}
				else {
					const uint32_t newOverflowPageNumber = metaData_.acquireOverflowPageNumber();
					OverflowDataPage& newOverflowPage = cache.newOverflowPage(newOverflowPageNumber);

					addedInlineRecordSize = newOverflowPage.addSingleRecord(recordId, addedValueRef);
					RAISE_INTERNAL_ERROR_IF(addedInlineRecordSize == 0, "unable to add record to %s", newOverflowPage.getId().toString());

					DataPage& parentPage = cache.dataPage(parentPageId);
					parentPage.setNextOverflowPage(newOverflowPageNumber);

					HASHDB_LOG_DEBUG_DETAIL("Added new %s record key=\"%s\" to bucket %u (new %s)", (isInlineRecord)? "inline" : "large", key.to_string(), bucketNumberForKey, newOverflowPage.getId().toString());

					// Split on overfill? (Aka "controlled split".)
					const bool overfill = metaData_.isOverfill(addedInlineRecordSize);
					if (overfill) {
						HASHDB_LOG_DEBUG_DETAIL("Overfill detected, actual fill=%u, expected fill=%u", metaData_.actualFill(addedInlineRecordSize), metaData_.expectedFill());
						splitOnOverfill(cache);
						metaData_.incrementOverfillStatistics();
					}
				}
			}
				
			metaData_.recordAdded(addedInlineRecordSize);
		}
	}

	size_type OpenDatabase::storeSingleValue(SingleRequestCache& cache, const IWriteBatch& writeBatch, size_t index)
	{
		const StringOrReference keyHolder = writeBatch.keyAt(index);
		const partNum_t partNum = writeBatch.partNumAt(index);
		const StringOrReference valueHolder = writeBatch.valueAt(index);

		const bool tooLarge = (storeThrowIfLargerThan_ != 0 && valueHolder.size() > storeThrowIfLargerThan_);

		if (! tooLarge) {
			storeSingleValue(cache, keyHolder.getRef(), partNum, valueHolder.getRef());
		}

		return (tooLarge)? 1 : 0;
	}

	void OpenDatabase::store(const IWriteBatch& writeBatch)
	{
		SingleRequestCache cache(environment_, openFiles_);

		const size_type batchSize = static_cast<size_type>(writeBatch.count());
		size_type numberOfTooLargeValues = 0;

		if (batchSize >= MIN_BATCH_SIZE_TO_REORDER && ! metaData_.isOverfill(MIN_FREE_SPACE_TO_REORDER)) {

			batchAccessVector_t accessOrder;
			createBatchAccessOrder(accessOrder, metaData_, writeBatch);

			for (batchAccessVector_t::iterator ii = accessOrder.begin(); ii != accessOrder.end(); ++ii) {
				numberOfTooLargeValues += storeSingleValue(cache, writeBatch, ii->index());
			}

		}
		else {

			for (size_type i = 0; i < batchSize; ++i) {
				numberOfTooLargeValues += storeSingleValue(cache, writeBatch, i);
			}

		}

		metaData_.save();

		RAISE_VALUE_TOO_LARGE_IF(numberOfTooLargeValues == 1, "unable to store value larger than store limit (%u bytes)", storeThrowIfLargerThan_);
		RAISE_VALUE_TOO_LARGE_IF(numberOfTooLargeValues > 1, "unable to store %u values larger than store limit (%u bytes)", numberOfTooLargeValues, storeThrowIfLargerThan_);
	}

	//-------------------------------------------------------------------------
	// Statistics.

	kerio::hashdb::Statistics OpenDatabase::statistics()
	{
		Statistics stats = metaData_.statistics();
		
		stats.cachedPages_ += environment_.pageAllocator()->heldPages();
		stats.cachedPages_ += 2; // Header pages.

		return stats;
	}

	//-------------------------------------------------------------------------
	// Support for iterators.

	bool OpenDatabase::iteratorFetch(IteratorPosition& position, std::string& key, partNum_t& partNum, std::string& value)
	{
		SingleRequestCache cache(environment_, openFiles_);
		bool success = false;

		while (! success && position.bucketNumber_ <= metaData_.highestBucket()) {

			while (! success && position.currentPageId_.isValid()) {
				DataPage& page = cache.dataPage(position.currentPageId_);
				DataPageCursor cursor(&page, position.recordIndex_);

				if (cursor.isValid()) {

					key = cursor.key().to_string();
					partNum = cursor.partNum();

					if (cursor.isInlineValue()) {
						value = cursor.inlineValue().to_string();
					}
					else {
						fetchLargeValue(value, cursor.largeValueSize(), cursor.firstLargeValuePageId());
					}

					cursor.next();
					position.recordIndex_ = cursor.index();
					success = true;

				}
				else {
					position.currentPageId_ = page.nextOverflowPageId();
					position.recordIndex_ = 0;
					incrementTraversedPages(position.pagesTraversedInOverflowChain_, position.currentPageId_);
				}
			}

			if (! success) {
				position.nextBucket();
			}

		}

		return success;
	}

	void OpenDatabase::incrementTraversedPages(size_type& numberOfTraversedPages, const PageId& id)
	{
		++numberOfTraversedPages;
		RAISE_DATABASE_CORRUPTED_IF(id.isValid() && numberOfTraversedPages > ALLOWED_OVERFLOW_CHAIN_MAX_SIZE, "cycle in overflow page chain (done %u page traversals) on %s", numberOfTraversedPages, id.toString());
	}

}; // namespace hashdb
}; // namespace kerio
