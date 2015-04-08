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

// HeaderPage.cpp - database header page.
#include "stdafx.h"
#include "utils/ConfigUtils.h"
#include "Version.h"
#include "HeaderPage.h"

namespace kerio {
namespace hashdb {

	//-------------------------------------------------------------------------

	void HeaderPage::setUp(uint32_t pageNumber)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(pageNumber != 0);
		Page::setUp(0);

		setDatabaseVersion(DATABASE_CURRENT_FORMAT_VERSION);
		setPageSize(size());
		setHighestPageNumber(0);
		setHighestBucket(static_cast<uint32_t>(-1)); // Bucket 0 is not created yet.
		setDatabaseNumberOfRecords(0);
		setDataSize(0);
	}

	void HeaderPage::validate() const
	{
		Page::validate();

		const uint32_t databaseFileVersion = getDatabaseVersion();
		if (databaseFileVersion < DATABASE_MINIMUM_FORMAT_VERSION || databaseFileVersion > DATABASE_CURRENT_FORMAT_VERSION) {
			RAISE_INCOMPATIBLE_DATABASE("file version %u cannot be opened by database version %u", databaseFileVersion, DATABASE_CURRENT_FORMAT_VERSION);
		}

		const uint32_t pageNumber = getPageNumber();
		RAISE_DATABASE_CORRUPTED_IF(pageNumber != 0, "bad page number %u on %s", pageNumber, getId().toString());

		const uint32_t pageSize = getPageSize();
		RAISE_DATABASE_CORRUPTED_IF(! isValidPageSize(pageSize), "bad page size %u on %s", pageSize, getId().toString());
	}

	//-------------------------------------------------------------------------
	// Utilities.

	uint32_t HeaderPage::computeChecksum() const
	{
		return xor32(HEADER_DATA_END);
	}


	void HeaderPage::updateChecksum()
	{
		setChecksum(0);
		const uint32_t newChecksum = computeChecksum();
		setChecksum(newChecksum);
	}

	//-------------------------------------------------------------------------
	// Field accessors.

	uint32_t HeaderPage::getDatabaseVersion() const
	{
		return get32unchecked(DATABASE_VERSION_OFFSET);
	}

	void HeaderPage::setDatabaseVersion(uint32_t databaseVersion)
	{
		put32unchecked(DATABASE_VERSION_OFFSET, databaseVersion);
	}

	uint32_t HeaderPage::getCreationTimestamp() const
	{
		return get32unchecked(CREATION_TIMESTAMP_OFFSET);
	}

	void HeaderPage::setCreationTimestamp(uint32_t creationTimestamp)
	{
		put32unchecked(CREATION_TIMESTAMP_OFFSET, creationTimestamp);
	}

	uint32_t HeaderPage::getCreationTag() const
	{
		return get32unchecked(CREATION_TAG_OFFSET);
	}

	void HeaderPage::setCreationTag(uint32_t creationTag)
	{
		put32unchecked(CREATION_TAG_OFFSET, creationTag);
	}

	uint32_t HeaderPage::getPageSize() const
	{
		return get32unchecked(PAGESIZE_OFFSET);
	}

	void HeaderPage::setPageSize(uint32_t pageSize)
	{
		put32unchecked(PAGESIZE_OFFSET, pageSize);
	}

	uint32_t HeaderPage::getHighestPageNumber() const
	{
		return get32unchecked(HIGHEST_PAGE_OFFSET);
	}

	void HeaderPage::setHighestPageNumber(uint32_t highestPage)
	{
		put32unchecked(HIGHEST_PAGE_OFFSET, highestPage);
	}

	uint32_t HeaderPage::getTestHash() const
	{
		return get32unchecked(THASH_OFFSET);
	}

	void HeaderPage::setTestHash(uint32_t hashTest)
	{
		put32unchecked(THASH_OFFSET, hashTest);
	}

	uint32_t HeaderPage::getHighestBucket() const
	{
		return get32unchecked(HIGHEST_BUCKET_OFFSET);
	}

	void HeaderPage::setHighestBucket(uint32_t highestBucket)
	{
		put32unchecked(HIGHEST_BUCKET_OFFSET, highestBucket);
	}

	uint64_t HeaderPage::getDatabaseNumberOfRecords() const
	{
		const uint64_t lo = get32unchecked(DATABASE_RECORDS_OFFSET_LO);
		const uint64_t hi = get32unchecked(DATABASE_RECORDS_OFFSET_HI);
		return lo | (hi << 32);
	}

	void HeaderPage::setDatabaseNumberOfRecords(uint64_t records)
	{
		put32unchecked(DATABASE_RECORDS_OFFSET_LO, static_cast<uint32_t>(records));
		put32unchecked(DATABASE_RECORDS_OFFSET_HI, static_cast<uint32_t>(records >> 32));
	}

	uint64_t HeaderPage::getDataSize() const
	{
		const uint64_t lo = get32unchecked(DATA_SIZE_OFFSET_LO);
		const uint64_t hi = get32unchecked(DATA_SIZE_OFFSET_HI);
		return lo | (hi << 32);
	}

	void HeaderPage::setDataSize(uint64_t dataSize)
	{
		put32unchecked(DATA_SIZE_OFFSET_LO, static_cast<uint32_t>(dataSize));
		put32unchecked(DATA_SIZE_OFFSET_HI, static_cast<uint32_t>(dataSize >> 32));
	}

}; // namespace hashdb
}; // namespace kerio
