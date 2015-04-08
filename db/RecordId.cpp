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

// RecordId.cpp - identifies record on a page.
#include "stdafx.h"
#include "utils/ExceptionCreator.h"
#include "RecordId.h"

namespace kerio {
namespace hashdb {

	RecordId::RecordId(const boost::string_ref& key, partNum_t partNum)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(key.empty() || key.size() > MAX_KEY_SIZE);
		RAISE_INTERNAL_ERROR_IF_ARG(partNum < 0 || partNum > MAX_PARTNUM);

		size_ = static_cast<size_type>(key.size()) + 2;

		buffer_[0] = static_cast<uint8_t>(key.size());
		memcpy(buffer_+1, key.data(), key.size());
		buffer_[size_ - 1] = static_cast<uint8_t>(partNum);
	}

	boost::string_ref RecordId::key() const
	{
		boost::string_ref keyVal = value();
		keyVal.remove_prefix(1);
		keyVal.remove_suffix(1);
		return keyVal;
	}

	const uint8_t* RecordId::data() const
	{
		return &(buffer_[0]);
	}

	size_type RecordId::size() const
	{
		return size_;
	}

	boost::string_ref RecordId::value() const
	{
		return boost::string_ref(reinterpret_cast<const char*>(&buffer_[0]), size_);
	}

	size_type RecordId::recordOverheadSize() const
	{
		return size() + sizeof(uint16_t); // Record id size + inline value size (2)
	}

}; // namespace hashdb
}; // namespace kerio
