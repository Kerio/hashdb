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

// Page.h - database page.
#pragma once
#include "utils/ExceptionCreator.h"
#include "SimplePageAllocator.h"
#include "PageId.h"

namespace kerio {
namespace hashdb {

	// HashDB uses the following page formats:
	//
	// * header page - first page of each HashDB file (see HeaderPage.h),
	// * bucket page - first page of each HashDB bucket, resides in the bucket file (see DataPage.h),
	// * overflow page - continuation of the bucket in the overflow file (see DataPage.h),
	// * big data page - a page in the overflow file containing data too large to fit to a single page,
	// * bitmap page - a page in the overflow file for managing overflow/large value allocations.
	//
	// All page types start with 3 common fields:
	//
	// offset size field
	//	0     4    page type magic number
	//	4     4    checksum or 0xffffffff if checksum is not implemented
	//	8     4    page number

	class Page { // intentionally copyable
	protected:
		static const uint32_t BUCKET_HEADER_MAGIC   = 0x078d75c8;
		static const uint32_t OVERFLOW_HEADER_MAGIC = 0x154fe1b7;

		static const uint32_t BUCKET_DATA_MAGIC     = 0x2e19d943;

		static const uint32_t OVERFLOW_DATA_MAGIC   = 0x35e2f297;
		static const uint32_t LARGE_VALUE_MAGIC     = 0x4dcf7a68;
		static const uint32_t BITMAP_MAGIC			= 0x51496e2b;

	public:
		typedef uint8_t value_type;

		Page(IPageAllocator* allocator, size_type size) 
			: memory_(allocator->allocate(size))
			, size_(size)
			, dirty_(false)
		{
			
		}

		virtual ~Page()
		{
			HASHDB_ASSERT(! dirty());
		}

		// Virtual methods.
		virtual uint32_t magic() const = 0;
		virtual PageId::DatabaseFile_t fileType() const = 0;

		virtual void setUp(uint32_t pageNumber);
		virtual void validate() const;

		// Page id.
		void setId(const PageId& pageId);
		const PageId& getId() const;
		void disown();

		// Size.
		size_type size() const
		{ 
			return size_; 
		}

		// Data accessors.
		value_type* mutableData()
		{
			dirty_ = true;
			return memory_.get();
		}

		const value_type* constData() const
		{
			return memory_.get();
		}

		value_type operator[](size_type index) const
		{
			RAISE_INTERNAL_ERROR_IF_ARG(index + sizeof(value_type) > size_);
			return constData()[index];
		}

		// Beware: calling [] for non-const page causes setting the dirty flag. Use get8() instead.
		value_type& operator[](size_type index)
		{
			RAISE_INTERNAL_ERROR_IF_ARG(index + sizeof(value_type) > size_);
			return mutableData()[index];
		}

		value_type get8(size_type index) const
		{
			return operator[](index);
		}

		void put8(size_type index, value_type value)
		{
			operator[](index) = value;
		}

		const uint16_t* constData16() const
		{
			return reinterpret_cast<const uint16_t*>(memory_.get());
		}

		uint16_t* mutableData16()
		{
			dirty_ = true;
			return reinterpret_cast<uint16_t*>(memory_.get());
		}

		uint16_t get16unchecked(size_type index) const
		{
			return constData16()[index >> 1];
		}

		void put16unchecked(size_type index, uint16_t value)
		{
			mutableData16()[index >> 1] = value;
		}

		uint16_t get16(size_type index) const
		{
			RAISE_INTERNAL_ERROR_IF_ARG(index + sizeof(uint16_t) > size_);
			return get16unchecked(index);
		}

		void put16(size_type index, uint16_t value)
		{
			RAISE_INTERNAL_ERROR_IF_ARG(index + sizeof(uint16_t) > size_);
			put16unchecked(index, value);
		}

		const uint32_t* constData32() const
		{
			return reinterpret_cast<const uint32_t*>(memory_.get());
		}

		uint32_t* mutableData32()
		{
			dirty_ = true;
			return reinterpret_cast<uint32_t*>(memory_.get());
		}

		uint32_t get32unchecked(size_type index) const
		{
			return constData32()[index >> 2];
		}

		void put32unchecked(size_type index, uint32_t value)
		{
			mutableData32()[index >> 2] = value;
		}

		uint32_t get32(size_type index) const
		{
			RAISE_INTERNAL_ERROR_IF_ARG(index + sizeof(uint32_t) > size_);
			return get32unchecked(index);
		}

		void put32(size_type index, uint32_t value)
		{
			RAISE_INTERNAL_ERROR_IF_ARG(index + sizeof(uint32_t) > size_);
			put32unchecked(index, value);
		}

		void putBytes(size_type index, boost::string_ref value);
		boost::string_ref getBytes(size_type index, size_type size) const;
		bool hasBytes(size_type index, boost::string_ref value) const;
		void moveBytes(size_type dstOffset, size_type srcOffset, size_type size);

		void clear();
		uint32_t xor32(size_type size) const;
		uint32_t xor32() const;

		// Dirty flag.
		bool dirty() const;
		void clearDirtyFlag();

		// Access to common fields.
	protected:
		static const uint16_t MAGIC_OFFSET = 0;
		static const uint16_t CHECKSUM_OFFSET = 4;
		static const uint16_t PAGENUM_OFFSET = 8;

	public:
		uint32_t getMagic() const;
		void setMagic(uint32_t magic);

		uint32_t getChecksum() const;
		void setChecksum(uint32_t checksum);

		uint32_t getPageNumber() const;
		void setPageNumber(uint32_t pageNumber);

	private:
		IPageAllocator::PageMemoryPtr memory_;
		size_type size_;
		PageId pageId_;
		bool dirty_;
	};

}; // namespace hashdb
}; // namespace kerio
