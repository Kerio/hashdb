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

// OverflowFilePageAllocator.h - manages pages in the overflow file.
#pragma once
#include  <boost/unordered_map.hpp>
#include "BitmapPage.h"
#include "OpenFiles.h"

namespace kerio {
namespace hashdb {

	class OverflowFilePageAllocator {
	public:
		OverflowFilePageAllocator(Environment& environment, OpenFiles& openFiles);
		uint32_t acquireOverflowPageNumber();
		void releaseOverflowPageNumber(uint32_t pageNumber);
		void save();

		uint32_t highestOverflowFilePage() const;
		size_type bitmapPagesAcquired() const;
		size_type bitmapPagesReleased() const;
		size_type overflowFileDataPages() const;
		size_type overflowFileBitmapPages() const;

		size_type heldPages() const;

	private:
		typedef boost::unordered_map<uint32_t, BitmapPage> bitmapPageMap_t;

		bitmapPageMap_t::iterator existingBitmapPage(uint32_t bitmapPageNumber);
		uint32_t acquireOffsetFromNewBitmapPage(uint32_t bitmapPageNumber);
		uint32_t acquireOffsetFromExistingBitmapPage(uint32_t bitmapPageNumber);

	private:
		bitmapPageMap_t loadedBitmaps_;

		uint32_t highestOverflowFilePage_;
		size_type bitmapPagesAcquired_;

		const size_type bitmapPageDistance_;

		Environment& environment_;
		OpenFiles& openFiles_;
	};


}; // namespace hashdb
}; // namespace kerio
