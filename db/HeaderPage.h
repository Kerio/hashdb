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

// HeaderPage.h - database header page.
#pragma once
#include <kerio/hashdb/Options.h>
#include "Page.h"

namespace kerio {
namespace hashdb {

	class HeaderPage : public Page { // intentionally copyable
		// Format of the buffer and overflow file header pages:
		// offset size accessors field
		//	0     4    Magic (Page): page type magic number (0x078d75c8 for bucket file header, 0x154fe1b7 for overflow file header)
		//	4     4    Checksum (Page): checksum or 0xffffffff if checksum is not implemented
		//	8     4    PageNumber (Page): page number = 0
		// 12     4    DatabaseVersion: format version of the current database
		// 16     4    CreationTimestamp: when the database was created
		// 20     4    CreationTag: a random value
		// 24     4    PageSize: page size
		// 28     4    HighestPageNumber: highest page number in this file
		// 32     4    TestHash: value of hash(testKey)
		// 36     4    HighestBucket: highest bucket in use (bucket file header)
		// 40     8    NumberOfRecords: number of records in the database (bucket file header)
		// 48     8    DataSize: size of key/value data in the database (bucket file header)

		static const uint16_t DATABASE_VERSION_OFFSET = 12;
		static const uint16_t CREATION_TIMESTAMP_OFFSET = 16;
		static const uint16_t CREATION_TAG_OFFSET = 20;
		static const uint16_t PAGESIZE_OFFSET = 24;
		static const uint16_t HIGHEST_PAGE_OFFSET = 28;
		static const uint16_t THASH_OFFSET = 32;
		static const uint16_t HIGHEST_BUCKET_OFFSET = 36;
		static const uint16_t DATABASE_RECORDS_OFFSET_LO = 40;
		static const uint16_t DATABASE_RECORDS_OFFSET_HI = 44;
		static const uint16_t DATA_SIZE_OFFSET_LO = 48;
		static const uint16_t DATA_SIZE_OFFSET_HI = 52;

	protected:
		static const uint16_t HEADER_DATA_END = 56; // end of header data

	public:

		HeaderPage(IPageAllocator* allocator, size_type size) 
			: Page(allocator, size)
		{

		}

		// Common methods.
		virtual void setUp(uint32_t pageNumber);
		virtual void validate() const;

		// Utilities.
		uint32_t computeChecksum() const;
		void updateChecksum();

		// Field accessors.
		uint32_t getDatabaseVersion() const;
		void setDatabaseVersion(uint32_t databaseVersion);

		uint32_t getCreationTimestamp() const;
		void setCreationTimestamp(uint32_t creationTimestamp);

		uint32_t getCreationTag() const;
		void setCreationTag(uint32_t creationTag);

		uint32_t getPageSize() const;
		void setPageSize(uint32_t pageSize);

		uint32_t getHighestPageNumber() const;
		void setHighestPageNumber(uint32_t highestPage);

		uint32_t getTestHash() const;
		void setTestHash(uint32_t hashTest);

		uint32_t getHighestBucket() const;
		void setHighestBucket(uint32_t highestBucket);

		uint64_t getDatabaseNumberOfRecords() const;
		void setDatabaseNumberOfRecords(uint64_t records);

		uint64_t getDataSize() const;
		void setDataSize(uint64_t dataSize);
	};


}; // namespace hashdb
}; // namespace kerio
