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

// OpenFiles.cpp - simple holder of the open database files.
#include "stdafx.h"
#include <kerio/hashdb/Constants.h>
#include "utils/ExceptionCreator.h"
#include "BucketHeaderPage.h"
#include "OverflowHeaderPage.h"
#include "OpenFiles.h"

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Opening & closing files (ctor and dtor)

	namespace {

		boost::filesystem::path createFileName(const boost::filesystem::path& database, const std::string& suffix)
		{
			boost::filesystem::path file(database);
			boost::filesystem::path extension(suffix);

			file += extension;
			return file;
		}

	};

	boost::filesystem::path OpenFiles::databaseNameToBucketFileName(const boost::filesystem::path& database)
	{
		return createFileName(database, ".dbb");
	}

	boost::filesystem::path OpenFiles::databaseNameToOverflowFileName(const boost::filesystem::path& database)
	{
		return createFileName(database, ".dbo");
	}

	OpenFiles::OpenFiles(const boost::filesystem::path& database, const Options& options, Environment& environment)
		: environment_(environment)
	{
		HASHDB_LOG_DEBUG("Opening database %s", database.string());

		try {
			
			boost::filesystem::path bucketFileName = databaseNameToBucketFileName(database);
			bucketFile_.reset(new PagedFile(bucketFileName, PageId::BucketFileType, options, environment));
			const PagedFile::fileSize_t bucketFileSize = bucketFile_->size();

			boost::filesystem::path overflowFileName = databaseNameToOverflowFileName(database);
			overflowFile_.reset(new PagedFile(overflowFileName, PageId::OverflowFileType, options, environment));
			const PagedFile::fileSize_t overflowFileSize = overflowFile_->size(); 

			isNew_ = (bucketFileSize == 0) && (overflowFileSize == 0);
			RAISE_DATABASE_CORRUPTED_IF(! isNew_ && bucketFileSize == 0, "bucket file is missing or empty");
			RAISE_DATABASE_CORRUPTED_IF(! isNew_ && bucketFileSize < MIN_PAGE_SIZE, "incomplete header in bucket file");
			RAISE_DATABASE_CORRUPTED_IF(! isNew_ && overflowFileSize == 0, "overflow file is missing or empty");
			RAISE_DATABASE_CORRUPTED_IF(! isNew_ && overflowFileSize < MIN_PAGE_SIZE, "incomplete header in overflow file");

			// New database -> create header pages.
			if (isNew_) {
				const std::string pathString = database.string();
				const uint32_t creationTag = options.hashFun_(pathString.c_str(), pathString.size());
				createHeaderPages(options, creationTag);
			}
			else {
				readHeaderPages();
				validate(options);
			}
		} catch (const std::exception& ex) {
			HASHDB_LOG_DEBUG("Error when opening database %s: %s", database.string(), ex.what());
			close();

			throw;
		}
	}

	void OpenFiles::close()
	{
		HASHDB_LOG_DEBUG("Closing database files");

		if (bucketFile_ && ! bucketFile_->isClosed()) {
			if (bucketFileHeader_) {
				saveBucketHeaderPage();
			}
			
			bucketFile_->close();
		}

		if (overflowFile_ && ! overflowFile_->isClosed()) {
			if (overflowFileHeader_) {
				saveOverflowHeaderPage();
			}
			
			overflowFile_->close();
		}
	}

	bool OpenFiles::isClosed() const
	{
		const bool bucketFileClosed = ! bucketFile_ || bucketFile_->isClosed();
		const bool overflowFileClosed = ! overflowFile_ || overflowFile_->isClosed();

		return bucketFileClosed && overflowFileClosed;
	}

	OpenFiles::~OpenFiles()
	{
		HASHDB_ASSERT(isClosed());

		if (! isClosed()) {
			try {
				close();
			}
			catch (std::exception&) {
				// Ignore
			}
		}
	}

	//----------------------------------------------------------------------------
	// File accessors.

	PagedFile* OpenFiles::bucketFile()
	{
		return bucketFile_.get();
	}

	PagedFile* OpenFiles::overflowFile()
	{
		return overflowFile_.get();
	}

	PagedFile* OpenFiles::file(PageId::DatabaseFile_t fileType)
	{
		return (fileType == PageId::BucketFileType)? bucketFile() : overflowFile();
	}

	//----------------------------------------------------------------------------
	// File accessors.

	void OpenFiles::write(Page& page)
	{
		file(page.getId().fileType())->write(page);
	}

	void OpenFiles::read(Page& page, const PageId& pageId)
	{
		file(pageId.fileType())->read(page, pageId);
	}

	void OpenFiles::sync()
	{
		bucketFile_->sync();
		overflowFile_->sync();
	}

	void OpenFiles::prefetch()
	{
		bucketFile_->prefetch();
		overflowFile_->prefetch();
	}

	//----------------------------------------------------------------------------
	// Header page accessors and utilities.
	
	HeaderPage* OpenFiles::bucketHeaderPage()
	{
		return bucketFileHeader_.get();
	}

	HeaderPage* OpenFiles::overflowHeaderPage()
	{
		return overflowFileHeader_.get();
	}

	void OpenFiles::saveBucketHeaderPage()
	{
		if (bucketFileHeader_->dirty()) {
			HASHDB_LOG_DEBUG("Saving dirty bucket file header page");
			bucketFileHeader_->updateChecksum();
			bucketFile_->write(*bucketFileHeader_);
		}
	}

	void OpenFiles::saveOverflowHeaderPage()
	{
		if (overflowFileHeader_->dirty()) {
			HASHDB_LOG_DEBUG("Saving dirty overflow file header page");
			overflowFileHeader_->updateChecksum();
			overflowFile_->write(*overflowFileHeader_);
		}
	}

	void OpenFiles::saveHeaderPages()
	{
		saveBucketHeaderPage();
		saveOverflowHeaderPage();
	}

	//----------------------------------------------------------------------------
	// Creating/processing header pages.

	void OpenFiles::createHeaderPages(const Options& options, uint32_t creationTag)
	{
		pageSize_ = options.pageSize_;

		HASHDB_LOG_DEBUG("Creating header pages, page size is %u", pageSize_);

		bucketFileHeader_.reset(new BucketHeaderPage(environment_.headerPageAllocator(), pageSize_));
		bucketFileHeader_->setUp(0);

		overflowFileHeader_.reset(new OverflowHeaderPage(environment_.headerPageAllocator(), pageSize_));
		overflowFileHeader_->setUp(0);

		const uint32_t testHash = options.computeTestHash();
		bucketFileHeader_->setTestHash(testHash);
		overflowFileHeader_->setTestHash(testHash);

		// Creation timestamp and creation tag should be the same for both bucket and overflow file.
		const uint32_t creationTimestamp = static_cast<uint32_t>(time(NULL));
		bucketFileHeader_->setCreationTimestamp(creationTimestamp);
		overflowFileHeader_->setCreationTimestamp(creationTimestamp);

		bucketFileHeader_->setCreationTag(creationTag);
		overflowFileHeader_->setCreationTag(creationTag);

		bucketFile_->write(*bucketFileHeader_);
		overflowFile_->write(*overflowFileHeader_);
	}

	void OpenFiles::readHeaderPages()
	{
		// Read minimal bucket header page.
		const PageId bucketHeaderId = bucketFilePage(0);
		bucketFileHeader_.reset(new BucketHeaderPage(environment_.headerPageAllocator(), MIN_PAGE_SIZE));

		bucketFile_->setPageSize(MIN_PAGE_SIZE);
		bucketFile_->read(*bucketFileHeader_, bucketHeaderId);
		bucketFileHeader_->validate();

		// Re-read the bucket file header page if it is bigger.
		pageSize_ = bucketFileHeader_->getPageSize();

		if (MIN_PAGE_SIZE != pageSize_) {
			bucketFileHeader_.reset(new BucketHeaderPage(environment_.headerPageAllocator(), pageSize_));

			bucketFile_->setPageSize(pageSize_);
			bucketFile_->read(*bucketFileHeader_, bucketHeaderId);
			bucketFileHeader_->validate();
		}

		// Read overflow file header page.
		const PageId overflowHeaderId = overflowFilePage(0);
		overflowFileHeader_.reset(new OverflowHeaderPage(environment_.headerPageAllocator(), pageSize_));

		overflowFile_->setPageSize(pageSize_);
		overflowFile_->read(*overflowFileHeader_, overflowHeaderId);
		overflowFileHeader_->validate();

		// Check that the page size matches.
		const size_type overflowPageSize = overflowFileHeader_->getPageSize();
		RAISE_DATABASE_CORRUPTED_IF(pageSize_ != overflowPageSize, "page size on bucket header page does not match page size on overflow header page (%u != %u)", pageSize_, overflowPageSize);

		// Check that creation timestamps/tags match in both headers.
		const uint32_t bucketCreationTimestamp = bucketFileHeader_->getCreationTimestamp();
		const uint32_t overflowCreationTimestamp = overflowFileHeader_->getCreationTimestamp();
		RAISE_DATABASE_CORRUPTED_IF(bucketCreationTimestamp != overflowCreationTimestamp, "creation timestamps in bucket and overflow header pages do not match (%08x != %08x)", bucketCreationTimestamp, overflowCreationTimestamp);

		const uint32_t bucketCreationTag = bucketFileHeader_->getCreationTag();
		const uint32_t overflowCreationTag = overflowFileHeader_->getCreationTag();
		RAISE_DATABASE_CORRUPTED_IF(bucketCreationTag != overflowCreationTag, "creation tags in bucket and overflow header pages do not match (%08x != %08x)", bucketCreationTag, overflowCreationTag);

		HASHDB_LOG_DEBUG("Read header pages, page size is %u", pageSize_);
	}

	void OpenFiles::validate(const Options& options) const
	{
		// Check that the hash function used to create the database matches.
		const uint32_t testHash = options.computeTestHash();
		RAISE_INVALID_ARGUMENT_IF(bucketFileHeader_->getTestHash() != testHash, "Hash function does not match on %s", bucketFileHeader_->getId().toString());
		RAISE_INVALID_ARGUMENT_IF(overflowFileHeader_->getTestHash() != testHash, "Hash function does not match on %s", overflowFileHeader_->getId().toString());

		// Check that file is not smaller than the maximum number of pages.
		const PagedFile::fileSize_t bucketFileSize = bucketFile_->size();
		const uint32_t bucketFilePages = bucketFileHeader_->getHighestPageNumber() + 1;
		RAISE_DATABASE_CORRUPTED_IF(bucketFileSize < (bucketFilePages * pageSize()), "missing pages in bucket file");

		const PagedFile::fileSize_t overflowFileSize = overflowFile_->size(); 
		const uint32_t overflowFilePages = overflowFileHeader_->getHighestPageNumber() + 1;
		RAISE_DATABASE_CORRUPTED_IF(overflowFileSize < (overflowFilePages * pageSize()), "missing pages in overflow file");
	}

	//----------------------------------------------------------------------------
	// State.

	bool OpenFiles::isNew() const
	{
		return isNew_;
	}

	size_type OpenFiles::pageSize() const
	{
		return pageSize_;
	}

}; // namespace hashdb
}; // namespace kerio
