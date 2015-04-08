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

// LargeValuePage.h - page containing part of large value data.
#pragma once
#include "RecordId.h"
#include "Page.h"

namespace kerio {
namespace hashdb {

	class LargeValuePage : public Page { // intentionally copyable
		// Format of the large value page:
		// offset size accessors field
		//	0     4    Magic (Page): page type magic number (0x2e19d943 for bucket page, 0x35e2f297 for overflow page)
		//	4     4    Checksum (Page): checksum or 0xffffffff if checksum is not implemented
		//	8     4    PageNumber (Page): page number
		// 12     4    NextLargeValuePage: next large value page number in the chain or zero
		// 16     n    start/continuation of the value

		static const uint16_t NEXT_LARGE_VALUE_PAGE_OFFSET = 12;

	protected:
		static const uint16_t HEADER_DATA_END_OFFSET = 16; // end of header data

	public:

		LargeValuePage(IPageAllocator* allocator, size_type size) 
			: Page(allocator, size)
		{

		}

		// Virtual methods.
		virtual uint32_t magic() const
		{
			return LARGE_VALUE_MAGIC;
		}

		virtual PageId::DatabaseFile_t fileType() const
		{
			return PageId::OverflowFileType;
		}

		virtual void setUp(uint32_t pageNumber);
		virtual void validate() const;

		// Utilities for data access.
		bool putValuePart(size_type& position, const boost::string_ref& value);
		bool getValuePart(std::string& value, size_type valueSize) const;

		// Large value chain traversal.
		PageId nextLargeValuePageId();

		// Field accessors.
		uint32_t getNextLargeValuePage() const;
		void setNextLargeValuePage(uint32_t pageNumber);

		size_type partSize(size_type remainderSize) const;
	};


}; // namespace hashdb
}; // namespace kerio
