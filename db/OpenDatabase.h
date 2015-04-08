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

// OpenDatabase.h - represents an open database.
#pragma once
#include <kerio/hashdb/HashDB.h>
#include "Environment.h"
#include "OpenFiles.h"
#include "MetaData.h"
#include "Vector.h"
#include "BucketDataPage.h"
#include "IteratorPosition.h"

namespace kerio {
namespace hashdb {

	class DataPage;
	class DataPageCursor;
	class SingleRequestCache;
	class SplitPages;

	class OpenDatabase : boost::noncopyable
	{
	public:
		static const size_type ASSUMED_OVERFLOW_CHAIN_MAX_SIZE = 10;
		static const size_type ALLOWED_OVERFLOW_CHAIN_MAX_SIZE = 1000;
		static const size_type MIN_BATCH_SIZE_TO_REORDER = 5;
		static const size_type MIN_FREE_SPACE_TO_REORDER = 512 * MIN_BATCH_SIZE_TO_REORDER;
		static const size_type ASSUMED_BATCH_SIZE_MAX_SIZE = 1000;

	public:
		OpenDatabase(const boost::filesystem::path& database, const Options& options);
		void close();
		bool isClosed() const;
		~OpenDatabase();

		void flush();
		void sync();
		void releaseSomeResources();
		void prefetch();

		void createInitialBucketPages(size_type initialBuckets);

		// Reading from the database.
		void fetchLargeValue(std::string& outValue, const size_type valueSize, const PageId& firstLargeValuePageId);
		bool fetchSingleValueAt(SingleRequestCache& cache, IReadBatch& readBatch, size_t index);

		// Deleting from the database.
		void freeLargeValuePages(const PageId& firstLargeValuePageId, size_type valueSize);
		void removeRecord(DataPage& page, DataPageCursor cursor);
		void removeSingleValue(SingleRequestCache& cache, const boost::string_ref& key, partNum_t partNum);
		void removeAllParts(SingleRequestCache& cache, const boost::string_ref& key);

		// Writing to the database.
	private:
		typedef Vector<uint32_t, ASSUMED_OVERFLOW_CHAIN_MAX_SIZE> OriginalOverflowPageNumbers_t;

		void splitToChains(SingleRequestCache& cache, OriginalOverflowPageNumbers_t& originalOverflowPageNumbers, SplitPages& chainBeingSplit, SplitPages& newChain);
		void splitOnOverfill(SingleRequestCache& cache);
		size_type splitAddRecordOnOverflow(SingleRequestCache& cache, const RecordId& recordId, const DataPage::AddedValueRef& valueRef);

	public:
		void storeSingleValue(SingleRequestCache& cache, const boost::string_ref& key, partNum_t partNum, const boost::string_ref& value);
		size_type storeSingleValue(SingleRequestCache& cache, const IWriteBatch& writeBatch, size_t index);

		// API methods.
		std::vector<partNum_t> listParts(const boost::string_ref& key);

		bool fetch(IReadBatch& readBatch);
		void remove(const IDeleteBatch& deleteBatch);
		void store(const IWriteBatch& writeBatch);

		Statistics statistics();

		// Support for iterators.
		bool iteratorFetch(IteratorPosition& position, std::string& key, partNum_t& partNum, std::string& value);

	private:
		void incrementTraversedPages(size_type& numberOfTraversedPages, const PageId& id);

		Environment environment_;
		OpenFiles openFiles_;
		MetaData metaData_;

		size_type storeThrowIfLargerThan_;
		size_type fetchIgnoreIfLargerThan_;
	};

}; // namespace hashdb
}; // namespace kerio
