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

// SingleThreadedPageAllocator.h - a page allocator for single-threaded use.
#pragma once
#include "Environment.h"

namespace kerio {
namespace hashdb {

	class SingleThreadedPageAllocatorCache;

	struct SingleThreadedPageAllocatorCacheEntry {
		SingleThreadedPageAllocatorCacheEntry();
		SingleThreadedPageAllocatorCacheEntry(IPageAllocator::value_type* pageMemory, IPageAllocator::counter_type* counterMemory);

		bool empty();
		void allocate(size_t pageSize);
		void free();

		IPageAllocator::value_type* pageMemory_;
		IPageAllocator::counter_type* counterMemory_;
	};

	class SingleThreadedPageAllocator : public IPageAllocator, boost::noncopyable
	{
	public:
		SingleThreadedPageAllocator(size_type maximumHeldBytes);

		virtual PageMemoryPtr allocate(size_type size);
		virtual void deallocate(value_type* pageMemory, counter_type* counterMemory);
		virtual void freeSomeMemory();
		virtual size_type heldPages();

	private:
		void init(size_type pageSize);

	private:
		boost::scoped_ptr<SingleThreadedPageAllocatorCache> cache_;
		const size_type maximumHeldBytes_;
		size_type pageSize_;
	};

}; // namespace hashdb
}; // namespace kerio
