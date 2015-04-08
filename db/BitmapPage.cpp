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
#include "stdafx.h"
#include "BitmapPage.h"

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Common methods.

	void BitmapPage::setUp(uint32_t pageNumber)
	{
		Page::setUp(pageNumber);

		setSeekStart(0);

		setChecksum(0xffffffff); // Not implemented for bitmap pages.
	}

	void BitmapPage::validate() const
	{
		Page::validate();

		const uint32_t seekStart = getSeekStart();
		RAISE_DATABASE_CORRUPTED_IF(seekStart > numberOfManagedPages(), "invalid seek start value %u on bitmap %s", seekStart, getId().toString());
	}

	//----------------------------------------------------------------------------
	// Accessors.

	uint32_t BitmapPage::getSeekStart() const
	{
		return get32unchecked(SEEK_START_OFFSET);
	}

	void BitmapPage::setSeekStart(uint32_t ix)
	{
		put32unchecked(SEEK_START_OFFSET, ix);
	}

	//----------------------------------------------------------------------------
	// Acquire/release.

	size_type BitmapPage::numberOfManagedPages(size_type pageSize)
	{
		return (pageSize - HEADER_DATA_END_OFFSET) * 8;
	}

	size_type BitmapPage::numberOfManagedPages() const
	{
		return numberOfManagedPages(size());
	}

#if defined _WIN32

	size_type BitmapPage::firstZeroInWord(uint32_t word) const
	{
		unsigned long index;
		_BitScanForward(&index, ~word);

		return index;
	}

#else

	size_type BitmapPage::firstZeroInWord(uint32_t word) const
	{
		const unsigned int mask = ~word;
		return __builtin_ffs(mask) - 1;
	}

#endif

	uint32_t BitmapPage::acquireAllocationBit(const uint32_t* const mapBegin, const uint32_t* freeAreaPtr)
	{
		const uint32_t currentStatusBits = *freeAreaPtr;
		const size_type bitNumber = firstZeroInWord(currentStatusBits);

		const size_type usedIndex = static_cast<size_type>(freeAreaPtr - constData32()) * sizeof(uint32_t);
		put32(usedIndex, currentStatusBits | (1 << bitNumber));

		return (static_cast<size_type>(freeAreaPtr - mapBegin) * 32) + bitNumber;
	}

	uint32_t BitmapPage::acquirePage(uint32_t)
	{
		const uint32_t seekStart = getSeekStart();
		RAISE_INTERNAL_ERROR_IF_ARG(seekStart > numberOfManagedPages());

		const uint32_t* const mapBegin = constData32() + (HEADER_DATA_END_OFFSET >> 2);
		const uint32_t* const mapEnd   = constData32() + (size() >> 2);
		uint32_t allocatedPageOffset = NO_SPACE;

		for (const uint32_t* ii = mapBegin + (seekStart / 32); ii != mapEnd; ++ii) {
			if (*ii != 0xffffffff) {
				allocatedPageOffset = acquireAllocationBit(mapBegin, ii);
				setSeekStart(allocatedPageOffset + 1);
				break;
			}
		}

		return allocatedPageOffset;
	}

	void BitmapPage::releasePage(uint32_t pageOffset)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(pageOffset >= numberOfManagedPages());

		const size_type usedIndex = HEADER_DATA_END_OFFSET + ((pageOffset / 32) << 2);
		const size_type usedBitNumber = pageOffset % 32;
		uint32_t bitMask = (1 << usedBitNumber);

		const uint32_t currentStatusBits = get32(usedIndex);
		RAISE_DATABASE_CORRUPTED_IF((currentStatusBits & bitMask) == 0, "page offset %u being released is not present in bitmap %s", pageOffset, getId().toString());
		put32unchecked(usedIndex, (currentStatusBits & ~bitMask));

		if (pageOffset < getSeekStart()) {
			setSeekStart(pageOffset);
		}
	}

}; // namespace hashdb
}; // namespace kerio
