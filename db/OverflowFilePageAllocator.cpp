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

// OverflowFilePageAllocator.cpp - manages pages in the overflow file.
#include "stdafx.h"
#include "OverflowFilePageAllocator.h"

namespace kerio {
namespace hashdb {

	OverflowFilePageAllocator::OverflowFilePageAllocator(Environment& environment, OpenFiles& openFiles) 
		: highestOverflowFilePage_(openFiles.overflowHeaderPage()->getHighestPageNumber())
		, bitmapPagesAcquired_(0)
		, bitmapPageDistance_(BitmapPage::numberOfManagedPages(openFiles.pageSize()) + 1)
		, environment_(environment)
		, openFiles_(openFiles)
	{

	}

	uint32_t OverflowFilePageAllocator::acquireOffsetFromNewBitmapPage(uint32_t bitmapPageNumber)
	{
		bitmapPageMap_t::value_type newBitmapPage(bitmapPageNumber, BitmapPage(environment_.pageAllocator(), openFiles_.pageSize()));
		newBitmapPage.second.setUp(bitmapPageNumber);
		const uint32_t pageNumberOffset = newBitmapPage.second.acquirePage();
		openFiles_.write(newBitmapPage.second);
		
		std::pair<bitmapPageMap_t::iterator, bool> insertResult = loadedBitmaps_.insert(newBitmapPage);
		RAISE_INTERNAL_ERROR_IF_ARG(! insertResult.second);

		++bitmapPagesAcquired_;
		return pageNumberOffset;
	}

	OverflowFilePageAllocator::bitmapPageMap_t::iterator OverflowFilePageAllocator::existingBitmapPage(uint32_t bitmapPageNumber)
	{
		OverflowFilePageAllocator::bitmapPageMap_t::iterator pageIt = loadedBitmaps_.find(bitmapPageNumber);
		if (pageIt == loadedBitmaps_.end()) {

			bitmapPageMap_t::value_type loadedPage(bitmapPageNumber, BitmapPage(environment_.pageAllocator(), openFiles_.pageSize()));
			openFiles_.read(loadedPage.second, overflowFilePage(bitmapPageNumber));
			loadedPage.second.validate();

			std::pair<bitmapPageMap_t::iterator, bool> insertResult = loadedBitmaps_.insert(loadedPage);
			RAISE_INTERNAL_ERROR_IF_ARG(! insertResult.second);

			pageIt = insertResult.first;
		}

		return pageIt;
	}

	uint32_t OverflowFilePageAllocator::acquireOffsetFromExistingBitmapPage(uint32_t bitmapPageNumber)
	{
		OverflowFilePageAllocator::bitmapPageMap_t::iterator pageIt = existingBitmapPage(bitmapPageNumber);
		return pageIt->second.acquirePage();
	}

	uint32_t OverflowFilePageAllocator::acquireOverflowPageNumber()
	{
		const uint32_t eventuallyCreatedPageNumber = highestOverflowFilePage_ + 1;
		RAISE_INVALID_ARGUMENT_IF(eventuallyCreatedPageNumber == 0, "database is full: overflow pages exhausted");

		uint32_t acquiredPageNumber = BitmapPage::NO_SPACE;
		for (size_type bitmapPageNumber = 1; bitmapPageNumber < eventuallyCreatedPageNumber; bitmapPageNumber += bitmapPageDistance_) {
			const uint32_t pageNumberOffset = acquireOffsetFromExistingBitmapPage(bitmapPageNumber);

			if (pageNumberOffset != BitmapPage::NO_SPACE) {
				acquiredPageNumber = bitmapPageNumber + 1 + pageNumberOffset; // 0 -> next page after bitmap page
				break;
			}
		}

		if (acquiredPageNumber == BitmapPage::NO_SPACE) {
			const uint32_t pageNumberOffset = acquireOffsetFromNewBitmapPage(eventuallyCreatedPageNumber);
			RAISE_INTERNAL_ERROR_IF_ARG(pageNumberOffset != 0);
			acquiredPageNumber = eventuallyCreatedPageNumber + 1;
		}

		if (acquiredPageNumber > highestOverflowFilePage_) {
			highestOverflowFilePage_ = acquiredPageNumber;
		}

		return acquiredPageNumber;
	}

	void OverflowFilePageAllocator::releaseOverflowPageNumber(uint32_t pageNumber)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(pageNumber == 0 || pageNumber > highestOverflowFilePage_);

		const uint32_t bitmapPageNumber = (((pageNumber - 1) / bitmapPageDistance_) * bitmapPageDistance_) + 1;
		const uint32_t pageNumberOffset = pageNumber - 1 - bitmapPageNumber;

		bitmapPageMap_t::iterator pageIt = existingBitmapPage(bitmapPageNumber);
		pageIt->second.releasePage(pageNumberOffset);
	}

	void OverflowFilePageAllocator::save()
	{
		for (OverflowFilePageAllocator::bitmapPageMap_t::iterator ii = loadedBitmaps_.begin(); ii != loadedBitmaps_.end(); ++ii) {
			openFiles_.write(ii->second);
		}
	}

	uint32_t OverflowFilePageAllocator::highestOverflowFilePage() const
	{
		return highestOverflowFilePage_;
	}

	size_type OverflowFilePageAllocator::bitmapPagesAcquired() const
	{
		return bitmapPagesAcquired_;
	}

	size_type OverflowFilePageAllocator::bitmapPagesReleased() const
	{
		return 0;
	}

	size_type OverflowFilePageAllocator::overflowFileDataPages() const
	{
		return (highestOverflowFilePage_ < 2)? 0 : highestOverflowFilePage_ - overflowFileBitmapPages();
	}

	size_type OverflowFilePageAllocator::overflowFileBitmapPages() const
	{
		return (highestOverflowFilePage_ < 2)? 0 : ((highestOverflowFilePage_ - 1) / bitmapPageDistance_) + 1;
	}


	size_type OverflowFilePageAllocator::heldPages() const
	{
		return static_cast<size_type>(loadedBitmaps_.size());
	}

}; // namespace hashdb
}; // namespace kerio
