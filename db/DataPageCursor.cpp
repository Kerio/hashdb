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

// DataPageCursor.cpp - low level cursor for reading records from a data page.
#include "stdafx.h"
#include "DataPageCursor.h"

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Ctor.

	DataPageCursor::DataPageCursor(DataPage* pagePtr, uint16_t recordIndex /*= 0*/ )
		: pagePtr_(pagePtr)
		, recordIndex_(recordIndex)
	{

	}

	//----------------------------------------------------------------------------
	// Traversal.

	void DataPageCursor::next()
	{
		++recordIndex_;
	}

	void DataPageCursor::reset()
	{
		recordIndex_ = 0;
	}

	bool DataPageCursor::find(const RecordId& recordId)
	{
		const uint16_t numberOfRecords = pagePtr_->getNumberOfRecords();
		bool found = false;

		for (; recordIndex_ < numberOfRecords; ++recordIndex_) {
			found = (recordIdValue() == recordId.value());

			if (found) {
				break;
			}
		}

		return found;
	}

	bool DataPageCursor::find(const boost::string_ref& searchKey)
	{
		const uint16_t numberOfRecords = pagePtr_->getNumberOfRecords();
		bool found = false;

		for (; recordIndex_ < numberOfRecords; ++recordIndex_) {
			found = (key() == searchKey);

			if (found) {
				break;
			}
		}

		return found;
	}

	//----------------------------------------------------------------------------
	// Cursor properties.

	bool DataPageCursor::isValid() const
	{
		return recordIndex_ < pagePtr_->getNumberOfRecords();
	}

	bool DataPageCursor::isInlineValue() const
	{
		const uint16_t recordOffset = this->recordOffset();
		return inlineValueSize(recordOffset) != INVALID_INLINE_VALUE_SIZE;
	}

	uint16_t DataPageCursor::index() const
	{
		return recordIndex_;
	}

	//----------------------------------------------------------------------------
	// Accessors.

	uint16_t DataPageCursor::recordOffset() const
	{
		return pagePtr_->getRecordOffsetAt(recordIndex_);
	}

	size_type DataPageCursor::recordIdSize(uint16_t recordOffset) const
	{
		return pagePtr_->operator[](recordOffset) + 2;
	}

	boost::string_ref DataPageCursor::recordIdValue() const
	{
		const uint16_t recordOffset = this->recordOffset();
		return pagePtr_->getBytes(recordOffset, recordIdSize(recordOffset));
	}

	boost::string_ref DataPageCursor::key() const
	{
		boost::string_ref id = recordIdValue();
		id.remove_prefix(1); // key size (1)
		id.remove_suffix(1); // part num (1)
		return id;
	}

	kerio::hashdb::partNum_t DataPageCursor::partNum() const
	{
		return recordIdValue().back();
	}

	uint16_t DataPageCursor::inlineValueSize(uint16_t recordOffset) const
	{
		const size_type valueSizeOffset = recordOffset + recordIdSize(recordOffset);
		return pagePtr_->operator[](valueSizeOffset) | (pagePtr_->operator[](valueSizeOffset + 1) << 8);
	}

	boost::string_ref DataPageCursor::inlineValue() const
	{
		boost::string_ref rv;

		const uint16_t recordOffset = this->recordOffset();
		const uint16_t valueSize = inlineValueSize(recordOffset);
		if (valueSize != INVALID_INLINE_VALUE_SIZE) {
			const size_type valueOffset = recordOffset + recordIdSize(recordOffset) + 2; // 2 bytes for value size
			rv = pagePtr_->getBytes(valueOffset, valueSize);
		}

		return rv;
	}

	boost::string_ref DataPageCursor::inlineRecord() const
	{
		const uint16_t recordOffset = this->recordOffset();
		const size_type recordIdSize = this->recordIdSize(recordOffset);
		const uint16_t inlineValueSize = this->inlineValueSize(recordOffset);
		const uint16_t inlineRecordDataSize = (inlineValueSize != INVALID_INLINE_VALUE_SIZE)? inlineValueSize : (2 * sizeof(uint32_t));
		
		return pagePtr_->getBytes(recordOffset, recordIdSize + 2 + inlineRecordDataSize);
	}

	uint32_t DataPageCursor::largeValueInfo(uint16_t recordOffset, size_type offset) const
	{
		size_type infoOffset = recordOffset + recordIdSize(recordOffset) + sizeof(uint16_t) + offset;
		
		const uint32_t rv = pagePtr_->operator[](infoOffset) 
			| (pagePtr_->operator[](infoOffset + 1) << 8)
			| (pagePtr_->operator[](infoOffset + 2) << 16)
			| (pagePtr_->operator[](infoOffset + 3) << 24);

		return rv;
	}

	size_type DataPageCursor::largeValueSize() const
	{
		const uint16_t recordOffset = this->recordOffset();
		return largeValueInfo(recordOffset, 0);
	}


	PageId DataPageCursor::firstLargeValuePageId() const
	{
		const uint16_t recordOffset = this->recordOffset();
		const uint32_t firstPage = largeValueInfo(recordOffset, 4);
		return overflowFilePage(firstPage);
	}

	size_type DataPageCursor::recordOverheadSize() const
	{
		const uint16_t recordOffset = this->recordOffset();
		return recordIdSize(recordOffset) + sizeof(uint16_t);
	}

}; // namespace hashdb
}; // namespace kerio
