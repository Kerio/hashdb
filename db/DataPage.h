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

// DataPage.h - page containing key/value data.
#pragma once
#include "RecordId.h"
#include "Page.h"

namespace kerio {
namespace hashdb {

	class DataPageCursor;

	class DataPage : public Page { // intentionally copyable
		// Format of bucket and overflow pages:
		// offset size accessors field
		//	0     4    Magic (Page): page type magic number (0x2e19d943 for bucket page, 0x35e2f297 for overflow page)
		//	4     4    Checksum (Page): checksum or 0xffffffff if checksum is not implemented
		//	8     4    PageNumber (Page): page number
		// 12     4    NextOverflowPage: next overflow page number in the chain or zero
		// 16     2    NumberOfRecords: number of records on the page
		// 18     2    EndOfFreeArea: highest free byte on page + 1
		// 20     2    record offset 0
		// 22     2    record offset 1
		// ...
		//
		// Each record has following format:
		//  offset size field
		//	0     1    key size (1..127)
		//	1     n    key
		//	n+1   1    part number (0..127)
		//	n+2   2    value size or 0xffff for big data
		//	n+4   -    value for small data; 4 byte size and 4 byte page pointer for big data

		static const uint16_t NEXT_OVERFLOW_PAGE_OFFSET = 12;
		static const uint16_t NUMBER_OF_RECORDS_OFFSET = 16;
		static const uint16_t HIGHEST_FREE_BYTE_OFFSET = 18;

	protected:
		static const uint16_t HEADER_DATA_END_OFFSET = 20; // end of header data

	public:

		DataPage(IPageAllocator* allocator, size_type size) 
			: Page(allocator, size)
		{

		}

		// Virtual methods.
		virtual void setUp(uint32_t pageNumber);
		virtual void validate() const;

		// Utilities for data access.
		static size_type dataSpace(size_type pageSize);
		size_type largestPossibleInlineRecordSize() const;
		size_type freeSpace() const;
		void addRecordOffset(size_type offset);

		class AddedValueRef { // Intentionally copyable.
		public:
			AddedValueRef(const boost::string_ref& value);
			AddedValueRef(size_type valueSize, const PageId& firstLargeValuePage);

			uint16_t valueSizeOrTag() const;
			boost::string_ref value() const;

		private:
			const uint16_t valueSizeOrTag_;
			const boost::string_ref valueReference_;
			const int64_t largeValueRef_;
		};

		size_type addSingleRecord(const RecordId& recordId, const AddedValueRef& valueRef);
		size_type addSingleRecord(const boost::string_ref& recordInlineData);
		size_type deleteSingleRecord(const DataPageCursor& cursor);

		PageId nextOverflowPageId();

		// Field accessors.
		uint32_t getNextOverflowPage() const;
		void setNextOverflowPage(uint32_t pageNumber);

		uint16_t getNumberOfRecords() const;
		void setNumberOfRecords(uint16_t numberOfRecords);

		size_type getEndOfFreeArea() const;
		void setEndOfFreeArea(size_type freeByteOffset);

		uint16_t getRecordOffsetAt(size_type index) const;
		void setRecordOffsetAt(size_type index, size_type offset);
	};


}; // namespace hashdb
}; // namespace kerio
