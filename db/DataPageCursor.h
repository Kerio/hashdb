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

// DataPageCursor.h - low level cursor for reading records from a data page.
#pragma once
#include "DataPage.h"

namespace kerio {
namespace hashdb {

	class DataPageCursor { // intentionally copyable
	public:
		static const uint16_t INVALID_INLINE_VALUE_SIZE = 0xffff;

		DataPageCursor(DataPage* pagePtr, uint16_t recordIndex = 0);

		// Traversal.
		void next();
		void reset();
		bool find(const RecordId& recordId);
		bool find(const boost::string_ref& key);

		// Cursor properties.
		bool isValid() const;
		bool isInlineValue() const;
		uint16_t index() const;

		// Accessors.
		boost::string_ref recordIdValue() const;
		boost::string_ref key() const;
		partNum_t partNum() const;
		boost::string_ref inlineValue() const;
		boost::string_ref inlineRecord() const;

		size_type largeValueSize() const;
		PageId firstLargeValuePageId() const;

		size_type recordOverheadSize() const;

	private:
		uint16_t recordOffset() const;
		size_type recordIdSize(uint16_t recordOffset) const;
		uint16_t inlineValueSize(uint16_t recordOffset) const;
		uint32_t largeValueInfo(uint16_t recordOffset, size_type offset) const;

	private:
		const DataPage* pagePtr_;
		uint16_t recordIndex_;
	};

}; // namespace hashdb
}; // namespace kerio
