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

// FileUtils.cpp - file utilities for tests.
#include "stdafx.h"

#if defined(_WIN32)
#include <shlobj.h>
#include <shellapi.h>
#else
#include <sstream>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

#include "db/BucketDataPage.h"
#include "db/PagedFile.h"
#include "FileUtils.h"


#if defined(_WIN32)
//-----------------------------------------------------------------------------
// Win32 version.

std::string getTestPath()
{
	char tempPathBuffer[MAX_PATH];

	const DWORD tempPathLen = GetTempPathA(MAX_PATH, tempPathBuffer);
	TS_ASSERT(tempPathLen < MAX_PATH);
	TS_ASSERT(tempPathLen != 0);

	return std::string(tempPathBuffer) + "HashDBTest";
}

void createTestDirectory()
{
	const std::string testPath = getTestPath();
	const BOOL mkdirSucceeded = CreateDirectoryA(testPath.c_str(), NULL);
	TS_ASSERT(mkdirSucceeded);
}

void removeTestDirectory()
{
	std::string testPath = getTestPath();
	testPath.append(1, '\0');

	SHFILEOPSTRUCTA op;

	op.hwnd = 0;
	op.wFunc = FO_DELETE;
	op.pFrom = testPath.c_str();
	op.pTo = NULL;
	op.fFlags = FOF_NOCONFIRMATION | FOF_NOERRORUI;
	op.hNameMappings = 0;
	op.lpszProgressTitle = NULL;

	SHFileOperationA(&op);

	// Check that test directory does not exist anymore.
	const DWORD directoryAttributes = GetFileAttributesA(testPath.c_str());
	TS_ASSERT_EQUALS(directoryAttributes, INVALID_FILE_ATTRIBUTES);
}

void deleteFile(const std::string& fileName)
{
	const BOOL unlinkSucceeded = DeleteFileA(fileName.c_str());
	TS_ASSERT(unlinkSucceeded);
}

#else // defined(_WIN32)
//-----------------------------------------------------------------------------
// POSIX version.

std::string getTestPath()
{
	const char* tmpDir = getenv("TMPDIR");
	const pid_t pid = getpid();

	std::ostringstream os;
	
	if (tmpDir != NULL) {
		os << tmpDir;
	}
	else {
		os << "/tmp";
	}
	
	os << "/HashDBTest" << pid;

	return os.str();
}

void createTestDirectory()
{
	const std::string testPath = getTestPath();
	const int mkdirResult = mkdir(testPath.c_str(), 0700);
	TS_ASSERT(mkdirResult == 0);
}

void removeTestDirectory()
{
	std::string testPath = getTestPath();

    DIR* dir = opendir(testPath.c_str());
    if (dir != NULL) {

        struct dirent* entry;
        while ((entry = readdir(dir))) {

            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }
      
            std::string deletedFile = testPath + "/" + entry->d_name;
            deleteFile(deletedFile);
        }

        closedir(dir);
        TS_ASSERT(rmdir(testPath.c_str()) == 0);
    }
}

void deleteFile(const std::string& fileName)
{
	const int unlinkResult = unlink(fileName.c_str());
	TS_ASSERT(unlinkResult == 0);
}

#endif // defined(_WIN32)

//-----------------------------------------------------------------------------
// Platform-independent code.

void corruptFile32(const std::string& fileName, kerio::hashdb::size_type pageSize, uint32_t pageNumber, uint32_t pageOffset, uint32_t newValue)
{
	using namespace kerio::hashdb;

	Options options = Options::readWriteSingleThreaded();
	options.pageSize_ = pageSize;
	options.createIfMissing_ = false;

	Environment environment(options);

	PagedFile pf(fileName, PageId::BucketFileType, options, environment);

	BucketDataPage page(environment.pageAllocator(), pageSize);
	pf.read(page, bucketFilePage(pageNumber));
	page.put32(pageOffset, newValue);
	pf.write(page);
	pf.close();
}

