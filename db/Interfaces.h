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

// Interfaces.h - internal interfaces.
#pragma once
#include <kerio/hashdb/HashDB.h>
#include "utils/Assert.h"
#include "PageId.h"

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Interface of the lock manager.

	class ILockManager {
	public:
		typedef uint32_t lockSet_t;

		virtual lockSet_t newLockSet() = 0;
		virtual void acquireReadLock(lockSet_t lockSet, const PageId& pageId) = 0;
		virtual void acquireWriteLock(lockSet_t lockSet, const PageId& pageId) = 0;
		virtual void releaseLockSet(lockSet_t lockSet) = 0;

		virtual ~ILockManager() { }
	};

	//----------------------------------------------------------------------------
	// Interface of the page allocator.

	class IPageAllocator {
	public:
		typedef uint8_t value_type;
		typedef size_type counter_type;

		class PageMemoryPtr {
		public:
			PageMemoryPtr(IPageAllocator* allocator, value_type* pageMemory, counter_type* counterMemory, counter_type initialCount = 1)
				: allocator_(allocator)
				, pageMemory_(pageMemory)
				, counterMemory_(counterMemory)
			{	
				HASHDB_ASSERT(allocator_ != NULL);
				HASHDB_ASSERT(pageMemory_ != NULL);
				HASHDB_ASSERT(counterMemory_ != NULL);
				HASHDB_ASSERT(initialCount != 0);

				*counterMemory = initialCount;
			}

			PageMemoryPtr(const PageMemoryPtr& ref)
				: allocator_(ref.allocator_)
				, pageMemory_(ref.pageMemory_)
				, counterMemory_(ref.counterMemory_) 
			{
				HASHDB_ASSERT(allocator_ != NULL);
				HASHDB_ASSERT(pageMemory_ != NULL);
				HASHDB_ASSERT(counterMemory_ != NULL);
				HASHDB_ASSERT(*counterMemory_ != 0);

				++(*counterMemory_);
			}

			PageMemoryPtr& operator=(const PageMemoryPtr& rhs)
			{
				HASHDB_ASSERT(rhs.allocator_ != NULL);
				HASHDB_ASSERT(rhs.pageMemory_ != NULL);
				HASHDB_ASSERT(rhs.counterMemory_ != NULL);
				HASHDB_ASSERT(*(rhs.counterMemory_) != 0);

				++(*rhs.counterMemory_);

				// Delete yourself if necessary.
				if (--(*counterMemory_) == 0) {
					deallocate();
				}

				// Copy rhs.
				allocator_ = rhs.allocator_;
				counterMemory_ = rhs.counterMemory_;
				pageMemory_ = rhs.pageMemory_;
				return *this;
			}

			~PageMemoryPtr()
			{
				HASHDB_ASSERT(allocator_ != NULL);
				HASHDB_ASSERT(pageMemory_ != NULL);
				HASHDB_ASSERT(counterMemory_ != NULL);
				HASHDB_ASSERT(*counterMemory_ != 0);

				if (--(*counterMemory_) == 0) {
					deallocate();
				}
			}

			value_type* get() {
				return pageMemory_;
			}

			value_type* get() const {
				return pageMemory_;
			}

		private:
			void deallocate()
			{
				allocator_->deallocate(pageMemory_, counterMemory_);
				
				allocator_ = NULL;
				pageMemory_ = NULL;
				counterMemory_ = NULL;
			}

			IPageAllocator* allocator_;
			value_type* pageMemory_;
			counter_type* counterMemory_;
		};

	public:
		virtual PageMemoryPtr allocate(size_type size) = 0;
		virtual void deallocate(value_type* pageMemory, counter_type* counterMemory) = 0;
		virtual void freeSomeMemory() = 0;
		virtual size_type heldPages() = 0;

		virtual ~IPageAllocator() { }
	};

	//----------------------------------------------------------------------------

}; // namespace hashdb
}; // namespace kerio
