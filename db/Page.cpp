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

// Page.cpp - database page.
#include "stdafx.h"
#include "Page.h"

namespace kerio {
namespace hashdb {

	void Page::setUp(uint32_t pageNumber)
	{
		const PageId pageId(fileType(), pageNumber);
		setId(pageId);

		clear();
		setMagic(magic());
		setPageNumber(pageNumber);
	}

	void Page::validate() const
	{
		RAISE_INTERNAL_ERROR_IF_ARG(fileType() != getId().fileType());

		const uint32_t pageMagicNumber = getMagic();
		RAISE_DATABASE_CORRUPTED_IF(pageMagicNumber != magic(), "bad magic number %08x on %s", pageMagicNumber, getId().toString());

		const uint32_t checksum = getChecksum();
		if (checksum != 0xffffffff) {
			const uint32_t computedChecksum = xor32();
			RAISE_DATABASE_CORRUPTED_IF(computedChecksum != 0, "bad checksum %08x on %s", computedChecksum, getId().toString());
		}

		const uint32_t idPageNumber = getId().pageNumber();
		const uint32_t actualPageNumber = getPageNumber();
		RAISE_DATABASE_CORRUPTED_IF(idPageNumber != actualPageNumber, "bad page number %u on %s", actualPageNumber, getId().toString());
	}

	void Page::setId(const PageId& pageId)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(pageId.fileType() != fileType());
		pageId_ = pageId;
	}

	const PageId& Page::getId() const
	{
		return pageId_;
	}

	void Page::disown()
	{
		pageId_ = PageId();
		clearDirtyFlag();
	}

	void Page::putBytes(size_type index, boost::string_ref value)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(index + value.size() > size_);
		memcpy(mutableData() + index, value.data(), value.size());
	}

	boost::string_ref Page::getBytes(size_type index, size_type size) const
	{
		RAISE_INTERNAL_ERROR_IF_ARG(index + size > size_);
		return boost::string_ref(reinterpret_cast<const char*>(constData() + index), size);
	}

	bool Page::hasBytes(size_type index, boost::string_ref value) const
	{
		RAISE_INTERNAL_ERROR_IF_ARG(index + value.size() > size_);
		return memcmp(constData() + index, value.data(), value.size()) == 0;
	}

	void Page::moveBytes(size_type dstOffset, size_type srcOffset, size_type size)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(dstOffset + size > size_);
		RAISE_INTERNAL_ERROR_IF_ARG(srcOffset + size > size_);
		memmove(mutableData() + dstOffset, constData() + srcOffset, size);
	}

	void Page::clear()
	{
		memset(mutableData(), 0, size_);
	}

	uint32_t Page::xor32(size_type size) const
	{
		const uint32_t* const end = constData32() + (size >> 2);

		uint32_t rv = 0;
		for (const uint32_t* ii = constData32(); ii != end; ++ii) {
			rv ^= *ii;
		}

		return rv;
	}

	uint32_t Page::xor32() const
	{
		return xor32(size_);
	}

	uint32_t Page::getMagic() const
	{
		return get32unchecked(MAGIC_OFFSET);
	}

	void Page::setMagic(uint32_t magic)
	{
		put32unchecked(MAGIC_OFFSET, magic);
	}

	uint32_t Page::getChecksum() const
	{
		return get32unchecked(CHECKSUM_OFFSET);
	}

	void Page::setChecksum(uint32_t checksum)
	{
		put32unchecked(CHECKSUM_OFFSET, checksum);
	}

	uint32_t Page::getPageNumber() const
	{
		return get32unchecked(PAGENUM_OFFSET);
	}

	void Page::setPageNumber(uint32_t pageNumber)
	{
		put32unchecked(PAGENUM_OFFSET, pageNumber);
	}

	//--------------------------------------------------------------------------
	// Hnadling the dirty flag.

	bool Page::dirty() const
	{
		return dirty_;
	}

	void Page::clearDirtyFlag()
	{
		dirty_ = false;
	}

}; // namespace hashdb
}; // namespace kerio
