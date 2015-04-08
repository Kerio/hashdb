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

// DataPage.cpp - page containing key/value data.
#include "stdafx.h"
#include "DataPageCursor.h"
#include "DataPage.h"

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Common methods.

	void DataPage::setUp(uint32_t pageNumber)
	{
		Page::setUp(pageNumber);

		setChecksum(0xffffffff); // Not implemented for data pages.
		setNextOverflowPage(0);
		setNumberOfRecords(0);
		setEndOfFreeArea(size());
	}

	void DataPage::validate() const
	{
		Page::validate();

		const uint32_t endOfFreeArea = getEndOfFreeArea();
		RAISE_DATABASE_CORRUPTED_IF(endOfFreeArea > size(), "bad \"end of free area\" %u (page size = %u) on %s", endOfFreeArea, size(), getId().toString());
		RAISE_DATABASE_CORRUPTED_IF(endOfFreeArea < HEADER_DATA_END_OFFSET, "bad \"end of free area\" %u (page header ends at %u) on %s", endOfFreeArea, uint16_t(HEADER_DATA_END_OFFSET), getId().toString());

		const uint16_t numberOfRecords = getNumberOfRecords();
		const size_type maxRecords = (size() - HEADER_DATA_END_OFFSET) / 2;
		RAISE_DATABASE_CORRUPTED_IF(numberOfRecords > maxRecords, "too many records (%u > %u) on %s", numberOfRecords, maxRecords, getId().toString());
	}

	//----------------------------------------------------------------------------
	// Utilities for data access.

	size_type DataPage::dataSpace(size_type pageSize)
	{
		return pageSize - HEADER_DATA_END_OFFSET;
	}

	size_type DataPage::largestPossibleInlineRecordSize() const
	{
		return dataSpace(size()) - sizeof(uint16_t);
	}

	size_type DataPage::freeSpace() const
	{
		const size_type recordPointerArraySize = getNumberOfRecords() * sizeof(uint16_t);
		const size_type bytesfree = getEndOfFreeArea() - (HEADER_DATA_END_OFFSET + recordPointerArraySize);
		
		RAISE_DATABASE_CORRUPTED_IF(bytesfree > size(), "free area %u > page size %u on %s", bytesfree, size(), getId().toString());
		return bytesfree;
	}

	void DataPage::addRecordOffset(size_type keyOffset)
	{
		const uint16_t numberOfRecords = getNumberOfRecords();
		setRecordOffsetAt(numberOfRecords, keyOffset);
		setNumberOfRecords(numberOfRecords + 1);
	}

	//----------------------------------------------------------------------------
	// Adding data.

	DataPage::AddedValueRef::AddedValueRef(const boost::string_ref& value) 
		: valueSizeOrTag_(static_cast<uint16_t>(value.size()))
		, valueReference_(value)
		, largeValueRef_(0LL)
	{ }

	DataPage::AddedValueRef::AddedValueRef(size_type valueSize, const PageId& firstLargeValuePage) 
		: valueSizeOrTag_(0xffff)
		, valueReference_(reinterpret_cast<const char*>(&largeValueRef_), sizeof(largeValueRef_))
		, largeValueRef_(valueSize | static_cast<uint64_t>(firstLargeValuePage.pageNumber()) << 32)
	{ 	}

	uint16_t DataPage::AddedValueRef::valueSizeOrTag() const
	{
		return valueSizeOrTag_;
	}

	boost::string_ref DataPage::AddedValueRef::value() const
	{
		return valueReference_;
	}

	size_type DataPage::addSingleRecord(const RecordId& recordId, const AddedValueRef& valueRef)
	{
		const size_type valueInlineSize = static_cast<size_type>(valueRef.value().size());
		const size_type recordInlineSize = recordId.recordOverheadSize() + valueInlineSize; // record overhead (record id size + inline value size (2)) + value or large value reference.
		const bool canAdd = freeSpace() >= sizeof(uint16_t) + recordId.recordOverheadSize() + valueInlineSize; // record pointer (2) + record.

		if (canAdd) {
			// Compute offsets.
			const size_type endOfFreeArea = getEndOfFreeArea();

			const size_type keyOffset = endOfFreeArea - recordInlineSize;
			const size_type valueOffset = endOfFreeArea - valueInlineSize;
			const size_type valueSizeOffset = valueOffset - sizeof(uint16_t);

			// Copy the key, value size and value.
			putBytes(keyOffset, recordId.value());

			const uint16_t valueSizeOrTag = valueRef.valueSizeOrTag();
			this->operator[](valueSizeOffset)     = (valueSizeOrTag & 0xff);
			this->operator[](valueSizeOffset + 1) = ((valueSizeOrTag >> 8) & 0xff);

			const boost::string_ref value = valueRef.value();
			putBytes(valueOffset, value);

			// Add new pointer to the start of the key.
			addRecordOffset(keyOffset);

			// Set new "end of free area".
			setEndOfFreeArea(keyOffset);
		}

		return (canAdd)? recordInlineSize : 0;
	}

	size_type DataPage::addSingleRecord(const boost::string_ref& recordInlineData)
	{
		const size_type recordInlineSize = static_cast<size_type>(recordInlineData.size());
		const bool canAdd = freeSpace() >= sizeof(uint16_t) + recordInlineSize;

		if (canAdd) {
			// Compute offsets.
			const size_type endOfFreeArea = getEndOfFreeArea();
			const size_type recordOffset = endOfFreeArea - recordInlineSize;

			// Copy the record, add pointer to its start and adjust "end of free area".
			putBytes(recordOffset, recordInlineData);
			addRecordOffset(recordOffset);
			setEndOfFreeArea(recordOffset);
		}

		return (canAdd)? recordInlineSize : 0;
	}

	//----------------------------------------------------------------------------
	// Deleting data.

	size_type DataPage::deleteSingleRecord(const DataPageCursor& cursor)
	{
		const uint16_t numberOfRecords = getNumberOfRecords();
		RAISE_INTERNAL_ERROR_IF_ARG(numberOfRecords == 0);
		RAISE_INTERNAL_ERROR_IF_ARG(! cursor.isValid());

		const size_type recordInlineSize = (cursor.isInlineValue())? 
			  cursor.recordOverheadSize() + static_cast<size_type>(cursor.inlineValue().size())	// inline value - overhead + value size
			: cursor.recordOverheadSize() + (2 * sizeof(uint32_t));								// big value - overhead + data size (4) + page pointer (4)

		if (cursor.index() < numberOfRecords - 1) {
			// Generic case: existing records and record pointers must be moved.
			const size_type endOfFreeArea = getEndOfFreeArea();

			const size_type movedBytes = getRecordOffsetAt(cursor.index()) - endOfFreeArea;
			const size_type newEndOfFreeArea = endOfFreeArea + recordInlineSize;

			// Move rest of the data to fill the hole.
			moveBytes(newEndOfFreeArea, endOfFreeArea, movedBytes);

			// Adjust record pointers.
			for (uint16_t i = cursor.index() + 1; i < numberOfRecords; ++i) {
				const size_type newOffset = getRecordOffsetAt(i) + recordInlineSize;
				setRecordOffsetAt(i - 1, newOffset);
			}

			setEndOfFreeArea(newEndOfFreeArea);
		}
		else {
			// Special case: adjust the end of free area when deleting the last item.
			const uint32_t newEndOfFreeArea = (numberOfRecords == 1)? size() : getRecordOffsetAt(cursor.index() - 1);
			setEndOfFreeArea(newEndOfFreeArea);
		}
		setNumberOfRecords(numberOfRecords - 1);

		return recordInlineSize;
	}

	//----------------------------------------------------------------------------
	// Overflow chain traversal.

	kerio::hashdb::PageId DataPage::nextOverflowPageId()
	{
		PageId rv;

		const uint32_t nextOverflowPage = getNextOverflowPage();
		if (nextOverflowPage != 0) {
			const PageId newPageId(overflowFilePage(nextOverflowPage));
			rv = newPageId;
		}

		return rv;
	}

	//----------------------------------------------------------------------------
	// Field accessors.

	uint32_t DataPage::getNextOverflowPage() const
	{
		return get32unchecked(NEXT_OVERFLOW_PAGE_OFFSET);
	}

	void DataPage::setNextOverflowPage(uint32_t pageNumber)
	{
		put32unchecked(NEXT_OVERFLOW_PAGE_OFFSET, pageNumber);
	}

	uint16_t DataPage::getNumberOfRecords() const
	{
		return get16unchecked(NUMBER_OF_RECORDS_OFFSET);
	}

	void DataPage::setNumberOfRecords(uint16_t numberOfRecords)
	{
		put16unchecked(NUMBER_OF_RECORDS_OFFSET, numberOfRecords);
	}

	size_type DataPage::getEndOfFreeArea() const
	{
		size_type freeByteOffset = get16unchecked(HIGHEST_FREE_BYTE_OFFSET);

		if (freeByteOffset == 0 && size() == MAX_PAGE_SIZE) {
			freeByteOffset = MAX_PAGE_SIZE; // Fix overflow.
		}

		return freeByteOffset;
	}

	void DataPage::setEndOfFreeArea(size_type freeByteOffset)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(freeByteOffset > size());

		put16unchecked(HIGHEST_FREE_BYTE_OFFSET, static_cast<uint16_t>(freeByteOffset));
	}

	uint16_t DataPage::getRecordOffsetAt(size_type index) const
	{
		RAISE_INTERNAL_ERROR_IF_ARG(index >= getNumberOfRecords());

		const size_type keyOffsetPosition = HEADER_DATA_END_OFFSET + (index * sizeof(uint16_t));
		return get16(keyOffsetPosition);
	}

	void DataPage::setRecordOffsetAt(size_type index, size_type offset)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(index > getNumberOfRecords());
		RAISE_INTERNAL_ERROR_IF_ARG(offset > size());

		const size_type keyOffsetPosition = HEADER_DATA_END_OFFSET + (index * sizeof(uint16_t));
		const uint16_t sixteenBitOffset = static_cast<uint16_t>(offset);
		put16(keyOffsetPosition, sixteenBitOffset);
	}

}; // namespace hashdb
}; // namespace kerio
