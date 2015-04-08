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

// PageId.h - identification of database pages.
#pragma once

namespace kerio {
namespace hashdb {

	class PageId {  // intentionally copyable
	public:
		enum DatabaseFile_t
		{
			InvalidFileType,
			BucketFileType,
			OverflowFileType
		};

		PageId()
			: fileType_(InvalidFileType)
			, pageNumber_(0)
		{

		}

		PageId(DatabaseFile_t fileType, uint32_t pageNumber)
			: fileType_(fileType)
			, pageNumber_(pageNumber)
		{

		}

		DatabaseFile_t fileType() const { return fileType_; }
		uint32_t pageNumber() const       { return pageNumber_; }
		bool isValid() const;

		bool operator==(const PageId& right) const
		{
			return (fileType_ == right.fileType_) && (pageNumber_ == right.pageNumber_);
		}

		bool operator!=(const PageId& right) const
		{
			return ! (*this == right);
		}

		bool operator<(const PageId& right) const
		{
			return (fileType_ < right.fileType_) 
				|| (fileType_ == right.fileType_ && pageNumber_ < right.pageNumber_);
		}

		bool operator>(const PageId& right) const
		{
			return (right < *this);
		}

		bool operator<=(const PageId& right) const
		{
			return ! (right < *this);
		}

		bool operator>=(const PageId& right) const
		{
			return ! (*this < right);
		}

		std::string toString() const;

	private:
		DatabaseFile_t fileType_;
		uint32_t pageNumber_;
	};

	PageId bucketFilePage(uint32_t pageNumber);
	PageId overflowFilePage(uint32_t pageNumber);

}; // namespace hashdb
}; // namespace kerio
