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

// BitmapPage.h - bitmap for page management in the overflow file.
#pragma once
#include <kerio/hashdb/Options.h>
#include "DataPage.h"

namespace kerio {
namespace hashdb {

	// Format of the bitmap page:
	// offset size field
	//	0     4    page type magic number (0x51496e2b)
	//	4     4    checksum or 0xffffffff if checksum is not implemented
	//	8     4    page number
	// 12     4    (SeekStart) bitmap offset to start seek from
	// 16     -    bitmap of next free pages (0 = free, 1 = allocated)

	class BitmapPage : public Page { // intentionally copyable
	protected:
		static const uint16_t SEEK_START_OFFSET = 12;

		static const uint16_t HEADER_DATA_END_OFFSET = 16; // end of header data

	public:
		static const uint32_t NO_SPACE = 0xffffffff;

	public:
		BitmapPage(IPageAllocator* allocator, size_type size) 
			: Page(allocator, size)
		{

		}

		virtual uint32_t magic() const
		{
			return BITMAP_MAGIC;
		}

		virtual PageId::DatabaseFile_t fileType() const
		{
			return PageId::OverflowFileType;
		}

		// Common methods
	public:
		void setUp(uint32_t pageNumber);
		void validate() const;

		// Accessors.
	public:
		uint32_t getSeekStart() const;
		void setSeekStart(uint32_t ix);

		// Acquire/release.
	public:
		static size_type numberOfManagedPages(size_type pageSize);
		size_type numberOfManagedPages() const;

		uint32_t acquirePage(uint32_t nearPageOffset = 0);
		void releasePage(uint32_t pageOffset);

	private:
		size_type firstZeroInWord(uint32_t word) const;
		uint32_t acquireAllocationBit(const uint32_t* const mapBegin, const uint32_t* freeAreaPtr);
	};

}; // namespace hashdb
}; // namespace kerio
