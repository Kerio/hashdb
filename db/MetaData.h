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

// MetaData.h - keeps metadata for database page allocation/deallocation.
#pragma once
#include <kerio/hashdb/Options.h>
#include <kerio/hashdb/Statistics.h>
#include "OverflowFilePageAllocator.h"

namespace kerio {
namespace hashdb {

	class OpenFiles;

    class MetaData {
	public:
        MetaData(Environment& environment, OpenFiles& openFiles, const Options& options);
    
        // Saving to header pages.
	public:
		void save(bool forceSave = false);

        // Management of the bucket file.
	public:
		uint32_t newBucketNumber();

		uint32_t bucketForKey(const boost::string_ref& key) const;
		uint32_t highestBucket() const;
		uint32_t bucketToSplit() const;


        // Management of the overflow file.
	public:
		uint32_t acquireOverflowPageNumber();
		void releaseOverflowPageNumber(uint32_t pageNumber);

		uint32_t acquireLargeValuePageNumber();
		void releaseLargeValuePageNumber(uint32_t pageNumber);

	private:
		uint32_t doAcquireOverflowFilePageNumber();
		void doReleaseOverflowFilePageNumber(uint32_t pageNumber);

        // Statistics.
	public:
		void recordAdded(size_type recordInlineSize);
		void recordRemoved(size_type recordInlineSize);
		void incrementOverfillStatistics();
		size_type actualFill(size_type recordInlineSize) const;
		size_type expectedFill() const;
		bool isOverfill(size_type recordInlineSize) const;

		Statistics statistics() const;

	private:
		void increaseSaveImportance();

	private:
		uint32_t highestBucket_; 
		uint32_t highMask_;			// Must be declared after highestBucket_, see the ctor.
		uint32_t bucketToSplit_;	// Must be declared after highMask_, see the ctor.
		uint64_t numberOfRecords_;
		uint64_t dataInlineSize_;
		int32_t leavePageFreeSpace_;

		OverflowFilePageAllocator overflowFileManager_;

		// Statistics.
		size_type bucketPagesAcquired_;
		size_type overflowPagesAcquired_;
		size_type largeValuePagesAcquired_;

		size_type bucketPagesReleased_;
		size_type overflowPagesReleased_;
		size_type largeValuePagesReleased_;

		size_type splitsOnOverfill_;

		Options::hashFun_t hashFun_;
		OpenFiles& openFiles_;

		size_type unsavedChanges_;
		size_type minFlushFrequency_;
    };

}; // namespace hashdb
}; // namespace kerio
