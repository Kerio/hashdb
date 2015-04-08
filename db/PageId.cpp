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

// PageId.cpp - identification of database pages.
#include "stdafx.h"
#include <sstream>
#include "utils/ExceptionCreator.h"
#include "PageId.h"

namespace kerio {
namespace hashdb {

	bool PageId::isValid() const
	{
		return fileType_ != InvalidFileType;
	}

	std::string PageId::toString() const
	{
		std::ostringstream os;
		
		if (pageNumber_ == 0) {
			os << "header page";
		}
		else {
			os << "page " << pageNumber_;
		}

		switch (fileType_) {
		case InvalidFileType:
			os << " not associated with a file";
			break;

		case BucketFileType:
			os << " of the bucket file";
			break;

		case OverflowFileType:
			os << " of the overflow file";
			break;
		}

		return os.str();
	}

	kerio::hashdb::PageId bucketFilePage(uint32_t pageNumber)
	{
		return PageId(PageId::BucketFileType, pageNumber);
	}

	kerio::hashdb::PageId overflowFilePage(uint32_t pageNumber)
	{
		return PageId(PageId::OverflowFileType, pageNumber);
	}

}; // namespace hashdb
}; // namespace kerio
