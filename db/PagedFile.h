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

// PagedFile.h - paged file.
#pragma once
#include "Page.h"

namespace kerio {
namespace hashdb {

	class PagedFile : boost::noncopyable {
	public:
		typedef uint64_t fileSize_t;

		PagedFile(const boost::filesystem::path& fileName, PageId::DatabaseFile_t fileType, const Options& options, Environment& environment);
		void close();
		bool isClosed() const;
		~PagedFile();

		void setPageSize(size_type newPageSize);
		size_type pageSize();
		fileSize_t size();
		void write(Page& page);
		void read(Page& page, const PageId& pageId);
		void sync();
		void prefetch();

	private:
		void doWrite(const Page& page);

	private:
		static const size_type PREFETCH_SIZE = 1 * 1024 * 1024; // Max prefetch size.

		const std::string fileName_;
		const PageId::DatabaseFile_t fileType_;

		size_type pageSize_;
		Environment& environment_;

#if defined _WIN32
		HANDLE file_;
#else
		int fd_;
#endif
	};

}; // namespace hashdb
}; // namespace kerio
