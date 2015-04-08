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

// SingleThreadedPageAllocator.cpp - a page allocator for single-threaded use.
#include "stdafx.h"
#include "utils/ExceptionCreator.h"
#include "utils/ConfigUtils.h"
#include "Vector.h"
#include "SingleThreadedPageAllocator.h"

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Cache entry.

	SingleThreadedPageAllocatorCacheEntry::SingleThreadedPageAllocatorCacheEntry() 
		: pageMemory_(NULL)
		, counterMemory_(NULL)
	{

	}

	SingleThreadedPageAllocatorCacheEntry::SingleThreadedPageAllocatorCacheEntry(IPageAllocator::value_type* pageMemory, IPageAllocator::counter_type* counterMemory)
		: pageMemory_(pageMemory)
		, counterMemory_(counterMemory)
	{

	}

	bool SingleThreadedPageAllocatorCacheEntry::empty()
	{
		return pageMemory_ == NULL;
	}

	void SingleThreadedPageAllocatorCacheEntry::allocate(size_t pageSize)
	{
		pageMemory_ = static_cast<IPageAllocator::value_type*>(malloc(pageSize));
		counterMemory_ = static_cast<IPageAllocator::counter_type*>(malloc(sizeof(IPageAllocator::counter_type)));

		if (pageMemory_ == NULL || counterMemory_ == NULL) {
			free();
			RAISE_INTERNAL_ERROR("Out of memory");
		}
	}

	void SingleThreadedPageAllocatorCacheEntry::free()
	{
		::free(pageMemory_);
		pageMemory_ = NULL;

		::free(counterMemory_);
		counterMemory_ = NULL;
	}

	//----------------------------------------------------------------------------
	// Cache.

	class SingleThreadedPageAllocatorCache {
	public:

		SingleThreadedPageAllocatorCache(size_type maximumHeldPages)
			: entries_(0)
			, maxEntries_(maximumHeldPages)
		{
			cache_.resize(maximumHeldPages);
		}

		bool push(SingleThreadedPageAllocatorCacheEntry entry)
		{
			const bool canPush = (entries_ < maxEntries_);

			if (canPush) {
				cache_[entries_++] = entry;
			}
			
			return canPush;
		}

		SingleThreadedPageAllocatorCacheEntry pop()
		{
			SingleThreadedPageAllocatorCacheEntry entry;

			if (entries_ > 0) {
				entry = cache_[--entries_];
			}

			return entry;
		}

		bool empty()
		{
			return entries_ == 0;
		}

		size_type entries()
		{
			return entries_;
		}

	private:
		Vector<SingleThreadedPageAllocatorCacheEntry, VECTOR_DEFAULT_STATIC_CAPACITY> cache_;
		size_type entries_;
		size_type maxEntries_;
	};

	//----------------------------------------------------------------------------
	// Allocator.

	SingleThreadedPageAllocator::SingleThreadedPageAllocator(size_type maximumHeldBytes)
		: maximumHeldBytes_(maximumHeldBytes)
		, pageSize_(0)
	{

	}

	void SingleThreadedPageAllocator::init(size_type pageSize)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(! isValidPageSize(pageSize));

		const size_type maxPages = maximumHeldBytes_ / pageSize;
		cache_.reset(new SingleThreadedPageAllocatorCache(maxPages));

		pageSize_ = pageSize;
	}

	IPageAllocator::PageMemoryPtr SingleThreadedPageAllocator::allocate(size_type size)
	{
		// Lazy initialization
		if (! cache_) {
			init(size);
		}

		RAISE_INTERNAL_ERROR_IF_ARG(pageSize_ != size);

		SingleThreadedPageAllocatorCacheEntry entry = cache_->pop();
		if (entry.empty()) {
			entry.allocate(size);
		}

		return PageMemoryPtr(this, entry.pageMemory_, entry.counterMemory_);
	}

	void SingleThreadedPageAllocator::deallocate(value_type* pageMemory, counter_type* counterMemory)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(! cache_);

		SingleThreadedPageAllocatorCacheEntry entry(pageMemory, counterMemory);
		
		if (! cache_->push(entry)) {
			entry.free();
		}
	}

	void SingleThreadedPageAllocator::freeSomeMemory()
	{
		if (cache_ && ! cache_->empty()) {
			init(pageSize_);
		}
	}

	kerio::hashdb::size_type SingleThreadedPageAllocator::heldPages()
	{
		size_type pages = 0;

		if (cache_) {
			pages = cache_->entries();
		}

		return pages;
	}

}; // namespace hashdb
}; // namespace kerio
