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

// LargeValuePage.cpp - page containing part of large value data.
#include "stdafx.h"
#include <algorithm>
#include "LargeValuePage.h"

#undef min // defined in windef.h

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Common methods.

	void LargeValuePage::setUp(uint32_t pageNumber)
	{
		Page::setUp(pageNumber);

		setChecksum(0xffffffff); // Not implemented for data pages.
		setNextLargeValuePage(0);
	}

	void LargeValuePage::validate() const
	{
		Page::validate();
	}

	//----------------------------------------------------------------------------
	// Utilities for data access.

	kerio::hashdb::size_type LargeValuePage::partSize(size_type remainderSize) const
	{
		const size_type largestPartSize = size() - HEADER_DATA_END_OFFSET;
		return std::min(remainderSize, largestPartSize);
	}

	bool LargeValuePage::putValuePart(size_type& position, const boost::string_ref& value)
	{
		const size_type remainingPartSize = static_cast<size_type>(value.size()) - position;

		if (remainingPartSize > 0) {
			const size_type putSize = partSize(remainingPartSize);
			putBytes(HEADER_DATA_END_OFFSET, value.substr(position, putSize));
			position += putSize;
		}

		return position < value.size();
	}

	bool LargeValuePage::getValuePart(std::string& value, size_type valueSize) const
	{
		const size_type remainingPartSize = valueSize - static_cast<size_type>(value.size());

		if (remainingPartSize > 0) {
			const size_type getSize = partSize(remainingPartSize);
			const boost::string_ref readPart = getBytes(HEADER_DATA_END_OFFSET, getSize);
			value.append(readPart.data(), readPart.size());
		}

		return value.size() < valueSize;
	}

	//----------------------------------------------------------------------------
	// Large value chain traversal.

	kerio::hashdb::PageId LargeValuePage::nextLargeValuePageId()
	{
		PageId rv;

		const uint32_t nextLargeValuePage = getNextLargeValuePage();
		if (nextLargeValuePage != 0) {
			const PageId newPageId(overflowFilePage(nextLargeValuePage));
			RAISE_DATABASE_CORRUPTED_IF(newPageId <= getId(), "cycle in large value page chain (page points to %u) on %s", nextLargeValuePage, getId().toString());
			rv = newPageId;
		}

		return rv;
	}

	//----------------------------------------------------------------------------
	// Field accessors.

	uint32_t LargeValuePage::getNextLargeValuePage() const
	{
		return get32unchecked(NEXT_LARGE_VALUE_PAGE_OFFSET);
	}

	void LargeValuePage::setNextLargeValuePage(uint32_t pageNumber)
	{
		put32unchecked(NEXT_LARGE_VALUE_PAGE_OFFSET, pageNumber);
	}

}; // namespace hashdb
}; // namespace kerio
