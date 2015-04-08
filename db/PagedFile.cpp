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

// PagedFile.cpp - database page.
#include "stdafx.h"

#if defined(_WIN32)
#include <Windows.h>
#else
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#endif

#include "utils/ExceptionCreator.h"
#include "utils/ConfigUtils.h"
#include "PagedFile.h"

namespace kerio {
namespace hashdb {

	//----------------------------------------------------------------------------
	// Platform independent methods.

	void PagedFile::setPageSize(size_type newPageSize)
	{
		RAISE_INTERNAL_ERROR_IF_ARG(! isValidPageSize(newPageSize));
		pageSize_ = newPageSize;
	}

	size_type PagedFile::pageSize()
	{
		return pageSize_;
	}

	void PagedFile::write(Page& page)
	{
		if (page.dirty()) {
			doWrite(page);
			page.clearDirtyFlag();
		}
	}

	PagedFile::~PagedFile()
	{
		HASHDB_ASSERT(isClosed());

		if (! isClosed()) {
			try {
				close();
			} catch (std::exception&) {
				// Ignore.
			}
		}
	}

#if defined _WIN32

	std::string describeIoError()
	{
		std::string errorDescription;
		LPSTR messageBuffer = NULL;

		const DWORD lastError = GetLastError(); 
		if (FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			lastError,
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
			(LPSTR) &messageBuffer,
			0,
			NULL))
		{
			errorDescription = messageBuffer;
			LocalFree(messageBuffer);
		}

		return errorDescription;
	}

	PagedFile::PagedFile(const boost::filesystem::path& fileName, PageId::DatabaseFile_t fileType, const Options& options, Environment& environment)
		: fileName_(fileName.string())
		, fileType_(fileType)
		, pageSize_(options.pageSize_)
		, environment_(environment)
		, file_(INVALID_HANDLE_VALUE)
	{
		// Guard clauses.
		RAISE_INTERNAL_ERROR_IF_ARG(fileName_.empty());
		RAISE_INTERNAL_ERROR_IF_ARG(fileType_ == PageId::InvalidFileType);
		options.validate();

		// Convert file name to Unicode.
		std::wstring path(fileName.native());

		// Open the file.
		const DWORD accessFlags = (options.readOnly_)? (GENERIC_READ) : (GENERIC_READ | GENERIC_WRITE);
		const DWORD shareFlags = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
		const DWORD creationFlags = (options.createIfMissing_)? OPEN_ALWAYS : OPEN_EXISTING;
		const DWORD attributesAndFlags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS;

		file_ = ::CreateFileW(path.c_str(), accessFlags, shareFlags, NULL, creationFlags, attributesAndFlags, NULL);

		// Fail on error.
		const bool openSucceeded = (file_ != INVALID_HANDLE_VALUE);
		const DWORD lastError = GetLastError();

		RAISE_IO_ERROR_IF(! openSucceeded, 
			"Unable to open%s file \"%s\"%s: %s", (options.createIfMissing_)? " or create" : "", fileName_, (options.readOnly_)? " read-only" : "", describeIoError());

		// Log the success.
		if (lastError == ERROR_ALREADY_EXISTS) {
			HASHDB_LOG_DEBUG("Created database file \"%s\"", fileName_);
		}
		else {
			HASHDB_LOG_DEBUG("Opened database file \"%s\"%s", fileName_, (options.readOnly_)? " read-only" : "");
		}
	}

	void PagedFile::close()
	{
		const BOOL closeSucceeded = ::CloseHandle(file_);
		RAISE_IO_ERROR_IF(! closeSucceeded, "unable to close database file \"%s\": %s", fileName_, describeIoError());

		file_ = INVALID_HANDLE_VALUE;
		HASHDB_LOG_DEBUG("Closed database file \"%s\"", fileName_);
	}

	bool PagedFile::isClosed() const
	{
		return file_ == INVALID_HANDLE_VALUE;
	}

	PagedFile::fileSize_t PagedFile::size()
	{
		LARGE_INTEGER fileSize;

		const BOOL getSizeSucceeded = GetFileSizeEx(file_, &fileSize);
		RAISE_IO_ERROR_IF(! getSizeSucceeded, "Unable to get size of database file \"%s\": %s", fileName_, describeIoError());

		return fileSize.QuadPart;
	}

	void PagedFile::doWrite(const Page& page)
	{
		// Guard clauses.
		const PageId& pageId = page.getId();
		RAISE_INTERNAL_ERROR_IF_ARG(pageId.fileType() != fileType_);
		RAISE_INTERNAL_ERROR_IF(page.size() != pageSize_, "Written page size %d differs from the page size %d of database file \"%s\"", page.size(), pageSize_, fileName_);

		// Compute the offset.
		const fileSize_t position = pageId.pageNumber() * static_cast<fileSize_t>(pageSize_);

		// Write.
		OVERLAPPED overlapped;
		memset(&overlapped, 0, sizeof(overlapped));
		overlapped.Offset = static_cast<DWORD>(position);
		overlapped.OffsetHigh = static_cast<DWORD>(position >> 32);

		DWORD bytesWritten = 0;
		const BOOL writeSucceeded = ::WriteFile(file_, page.constData(), pageSize_, &bytesWritten, &overlapped);

		// Fail on write error.
		RAISE_IO_ERROR_IF(! writeSucceeded, "Unable to write page %u to database file \"%s\": %s", pageId.pageNumber(), fileName_, describeIoError());
		RAISE_IO_ERROR_IF(bytesWritten != pageSize_, "Unable to write page %u to database file \"%s\": only %u of %u bytes written", pageId.pageNumber(), fileName_, bytesWritten, pageSize_);
	}

	void PagedFile::read(Page& page, const PageId& pageId)
	{
		// Guard clauses.
		RAISE_INTERNAL_ERROR_IF_ARG(pageId.fileType() != fileType_);
		RAISE_INTERNAL_ERROR_IF(page.size() != pageSize_, "Read page size %d differs from the page size %d of database file \"%s\"", page.size(), pageSize_, fileName_);

		// Compute the offset.
		const fileSize_t position = pageId.pageNumber() * static_cast<fileSize_t>(pageSize_);

		// Read.
		OVERLAPPED overlapped;
		memset(&overlapped, 0, sizeof(overlapped));
		overlapped.Offset = static_cast<DWORD>(position);
		overlapped.OffsetHigh = static_cast<DWORD>(position >> 32);

		DWORD bytesRead = 0;
		const BOOL readSucceeded = ::ReadFile(file_, page.mutableData(), pageSize_, &bytesRead, &overlapped);

		// Fail on read error.
		RAISE_IO_ERROR_IF(! readSucceeded, "Unable to read page %u from database file \"%s\": %s", pageId.pageNumber(), fileName_, describeIoError());
		RAISE_IO_ERROR_IF(bytesRead != pageSize_, "Unable to read page %u from database file \"%s\": only %u of %u bytes read", pageId.pageNumber(), fileName_, bytesRead, pageSize_);

		// Set page id.
		page.setId(pageId);

		// Clear dirty flag.
		page.clearDirtyFlag();
	}

	void PagedFile::sync()
	{
		const BOOL flushSucceeded = ::FlushFileBuffers(file_);
		if (! flushSucceeded) {
			HASHDB_LOG_DEBUG("Unable to flush database file \"%s\": %s", fileName_, describeIoError());
		}
	}

#else

	std::string describeIoError()
	{
		char* description = strerror(errno);
		std::string rv;

		if (description != NULL) {
			rv = description;
		}

		return rv;
	}

	PagedFile::PagedFile(const boost::filesystem::path& fileName, PageId::DatabaseFile_t fileType, const Options& options, Environment& environment)
		: fileName_(fileName.string())
		, fileType_(fileType)
		, pageSize_(options.pageSize_)
		, environment_(environment)
		, fd_(-1)
	{
		// Guard clauses.
		RAISE_INTERNAL_ERROR_IF_ARG(fileName_.empty());
		RAISE_INTERNAL_ERROR_IF_ARG(fileType_ == PageId::InvalidFileType);
		options.validate();

		// Open the file.
		const int accessFlags = (options.readOnly_)? O_RDONLY : O_RDWR;
		const int creationFlags = (options.createIfMissing_)? O_CREAT : 0;

#if defined _LINUX
		const int platformFlags = O_NOATIME;
#else
		const int platformFlags = 0;
#endif

		const int openFlags = accessFlags | creationFlags | platformFlags;
		fd_ = ::open(fileName.c_str(), openFlags, 0600);

		// Fail on error.
		RAISE_IO_ERROR_IF(fd_ == -1, "Unable to open%s file \"%s\"%s: %s", (options.createIfMissing_)? " or create" : "", fileName_, (options.readOnly_)? " read-only" : "", describeIoError());

		// Log the success.
		HASHDB_LOG_DEBUG("Opened database file \"%s\"%s", fileName_, (options.readOnly_)? " read-only" : "");
	}

	void PagedFile::close()
	{
		const int closeResult = ::close(fd_);
		RAISE_IO_ERROR_IF(closeResult != 0, "unable to close database file \"%s\": %s", fileName_, describeIoError());

		fd_ = -1;
		HASHDB_LOG_DEBUG("Closed database file \"%s\"", fileName_);
	}

	bool PagedFile::isClosed() const
	{
		return fd_ == -1;
	}

	PagedFile::fileSize_t PagedFile::size()
	{
		struct stat statbuf;
		
		const int statResult = ::fstat(fd_, &statbuf);
		RAISE_IO_ERROR_IF(statResult != 0, "Unable to get size of database file \"%s\": %s", fileName_, describeIoError());

		return statbuf.st_size;
	}

	void PagedFile::doWrite(const Page& page)
	{
		// Guard clauses.
		const PageId& pageId = page.getId();
		RAISE_INTERNAL_ERROR_IF_ARG(pageId.fileType() != fileType_);
		RAISE_INTERNAL_ERROR_IF(page.size() != pageSize_, "Written page size %d differs from the page size %d of database file \"%s\"", page.size(), pageSize_, fileName_);

		// Compute the offset.
		const off_t offset = pageId.pageNumber() * pageSize_;

		// Write.
		ssize_t writeResult = ::pwrite(fd_, page.constData(), pageSize_, offset);

		// Fail on write error.
		RAISE_IO_ERROR_IF(writeResult == -1, "Unable to write page %u to database file \"%s\": %s", pageId.pageNumber(), fileName_, describeIoError());
		RAISE_IO_ERROR_IF(static_cast<size_type>(writeResult) != pageSize_, "Unable to write page %u to database file \"%s\": only %u of %u bytes written", pageId.pageNumber(), fileName_, writeResult, pageSize_);
	}

	void PagedFile::read(Page& page, const PageId& pageId)
	{
		// Guard clauses.
		RAISE_INTERNAL_ERROR_IF_ARG(pageId.fileType() != fileType_);
		RAISE_INTERNAL_ERROR_IF(page.size() != pageSize_, "Read page size %d differs from the page size %d of database file \"%s\"", page.size(), pageSize_, fileName_);

		// Compute the offset.
		const off_t offset = pageId.pageNumber() * pageSize_;

		// Read.
		ssize_t readResult = ::pread(fd_, page.mutableData(), pageSize_, offset);

		// Fail on read error.
		RAISE_IO_ERROR_IF(readResult == -1, "Unable to read page %u from database file \"%s\": %s", pageId.pageNumber(), fileName_, describeIoError());
		RAISE_IO_ERROR_IF(static_cast<size_type>(readResult) != pageSize_, "Unable to read page %u from database file \"%s\": only %u of %u bytes read", pageId.pageNumber(), fileName_, readResult, pageSize_);

		// Set page id.
		page.setId(pageId);

		// Clear dirty flag.
		page.clearDirtyFlag();
	}

	void PagedFile::sync()
	{
		const int syncResult = ::fsync(fd_);
		if (syncResult != 0) {
			HASHDB_LOG_DEBUG("Unable to flush database file \"%s\": %s", fileName_, describeIoError());
		}
	}

#endif

#if defined _WIN32

	void PagedFile::prefetch()
	{
		// Windows does not have public API for file prefetch, try to do it manually.

		if (SetFilePointer(file_, 0, NULL, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
			HASHDB_LOG_DEBUG("Unable to prefetch file \"%s\": unable to reset pointer: %s", fileName_, describeIoError());
		}
		else {
			static const size_type PREFETCH_BUFFER_SIZE = 8 * 1024;
			size_type iterations = PREFETCH_SIZE / PREFETCH_BUFFER_SIZE;
			size_type actualPrefetchSize = 0;

			char buffer[PREFETCH_BUFFER_SIZE];

			for (size_type i = 0; i < iterations; ++i) {
				DWORD bytesRead;

				if (::ReadFile(file_, buffer, PREFETCH_BUFFER_SIZE, &bytesRead, NULL) == FALSE) {
					HASHDB_LOG_DEBUG("Unable to complete prefetch after %u bytes of file \"%s\": %s", actualPrefetchSize, fileName_, describeIoError());
					break;
				}

				if (bytesRead == 0) { // EOF
					HASHDB_LOG_DEBUG("Synchronously prefetched %u bytes of file \"%s\"", actualPrefetchSize, fileName_);
					break;
				}

				actualPrefetchSize += bytesRead;
			}
		}
	}

#elif defined _LINUX

	void PagedFile::prefetch()
	{
		if (posix_fadvise(fd_, 0, PREFETCH_SIZE, POSIX_FADV_WILLNEED) != 0) {
			HASHDB_LOG_DEBUG("Unable to prefetch file \"%s\": %s", fileName_, describeIoError());
		}
		else {
			HASHDB_LOG_DEBUG("Started asynchronous prefetch of %u bytes of file \"%s\"", size_type(PREFETCH_SIZE), fileName_);
		}
	}

#elif defined _MACOS

	void PagedFile::prefetch()
	{
		struct radvisory prefetch;
		prefetch.ra_offset = 0;
		prefetch.ra_count = static_cast<int>(PREFETCH_SIZE);

		if (fcntl(fd_, F_RDADVISE, &prefetch) == -1) {
			HASHDB_LOG_DEBUG("Unable to prefetch file \"%s\": %s", fileName_, describeIoError());
		}
		else {
			HASHDB_LOG_DEBUG("Started asynchronous prefetch of %u bytes of file \"%s\"", size_type(PREFETCH_SIZE), fileName_);
		}
	}

#endif

}; // namespace hashdb
}; // namespace kerio
