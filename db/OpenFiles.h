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

// OpenFiles.h - simple holder of the open database files.
#pragma once
#include "BucketHeaderPage.h"
#include "OverflowHeaderPage.h"
#include "PagedFile.h"

namespace kerio {
namespace hashdb {

	class OpenFiles : boost::noncopyable
	{
	public:
		OpenFiles(const boost::filesystem::path& database, const Options& options, Environment& environment);
		void close();
		bool isClosed() const;
		~OpenFiles();

		// File accessors.
		PagedFile* bucketFile();
		PagedFile* overflowFile();
		PagedFile* file(PageId::DatabaseFile_t fileType);

		// Header page accessors and utilities.
		HeaderPage* bucketHeaderPage();
		HeaderPage* overflowHeaderPage();
		void saveBucketHeaderPage();
		void saveOverflowHeaderPage();
		void saveHeaderPages();

		// File methods.
		void write(Page& page);
		void read(Page& page, const PageId& pageId);
		void sync();
		void prefetch();

		// State.
		bool isNew() const;
		size_type pageSize() const;

		// Utilities
		static boost::filesystem::path databaseNameToBucketFileName(const boost::filesystem::path& database);
		static boost::filesystem::path databaseNameToOverflowFileName(const boost::filesystem::path& database);

	private:
		// Creating/processing header pages.
		void createHeaderPages(const Options& options, uint32_t creationTag);
		void readHeaderPages();
		void validate(const Options& options) const;

	private:
		bool isNew_;
		size_type pageSize_;

		boost::scoped_ptr<PagedFile> bucketFile_;
		boost::scoped_ptr<BucketHeaderPage> bucketFileHeader_;

		boost::scoped_ptr<PagedFile> overflowFile_;
		boost::scoped_ptr<OverflowHeaderPage> overflowFileHeader_;

		Environment& environment_;
	};

}; // namespace hashdb
}; // namespace kerio
