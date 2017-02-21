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

// DatabaseImpl.cpp - implementation of the IDatabase interface.
#include "stdafx.h"
#include <boost/filesystem.hpp>
#include <kerio/hashdb/Constants.h>
#include <sstream>
#include "utils/SingleRead.h"
#include "utils/SingleWrite.h"
#include "utils/SingleDelete.h"
#include "IteratorImpl.h"
#include "OpenFiles.h"
#include "DatabaseImpl.h"

namespace kerio {
namespace hashdb {

	//-------------------------------------------------------------------------
	// Creation and destruction.

	Database DatabaseFactory()
	{
		Database db(new DatabaseImpl());
		return db;
	}

	DatabaseImpl::DatabaseImpl()
	{

	}

	DatabaseImpl::~DatabaseImpl()
	{
		try {
			close();
		} catch (std::exception&) {
			// Ignore.
		}
	}

	//-------------------------------------------------------------------------
	// Open and close.

	void DatabaseImpl::open(const boost::filesystem::path& database, const Options& options)
	{
		RAISE_INVALID_ARGUMENT_IF(database.empty(), "database name is empty");
		options.validate();
		RAISE_INVALID_ARGUMENT_IF(openDatabase_, "database is already open");

		openDatabase_.reset(new OpenDatabase(database, options));
	}

	void DatabaseImpl::close()
	{
		if (openDatabase_) {
			openDatabase_->close();
			openDatabase_.reset();
		}
	}

	void DatabaseImpl::flush()
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		openDatabase_->flush();
	}

	void DatabaseImpl::sync()
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		openDatabase_->sync();
	}

	void DatabaseImpl::releaseSomeResources()
	{
		if (openDatabase_) {
			openDatabase_->releaseSomeResources();
		}
	}

	void DatabaseImpl::prefetch()
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		openDatabase_->prefetch();
	}

	//-------------------------------------------------------------------------
	// Simple API methods.

	void DatabaseImpl::checkSimpleArgumentFor(const boost::string_ref& key, partNum_t partNum) const
	{
		RAISE_INVALID_ARGUMENT_IF(key.size() > MAX_KEY_SIZE, "key too long");
		RAISE_INVALID_ARGUMENT_IF(key.empty(), "empty key is not allowed");
		RAISE_INVALID_ARGUMENT_IF(partNum > MAX_PARTNUM, "partNum is too large");
		RAISE_INVALID_ARGUMENT_IF(partNum == ALL_PARTS, "partNum ALL_PARTS is not allowed");
		RAISE_INVALID_ARGUMENT_IF(partNum < 0, "negative partNum is not allowed");
	}

	bool DatabaseImpl::fetch(const boost::string_ref& key, partNum_t partNum, std::string& value)
	{
		checkSimpleArgumentFor(key, partNum);
		SingleRead singleRead(key, partNum, value);
		return doFetch(singleRead);
	}

	void DatabaseImpl::store(const boost::string_ref& key, partNum_t partNum, const boost::string_ref& value)
	{
		checkSimpleArgumentFor(key, partNum);
		SingleWrite singleWrite(key, partNum, value);
		doStore(singleWrite);
	}

	void DatabaseImpl::remove(const boost::string_ref& key, partNum_t partNum)
	{
		checkSimpleArgumentFor(key, partNum);
		SingleDelete singleDelete(key, partNum);
		doRemove(singleDelete);
	}

	void DatabaseImpl::remove(const boost::string_ref& key)
	{
		checkSimpleArgumentFor(key, 0);
		SingleDelete singleDelete(key, ALL_PARTS);
		doRemove(singleDelete);
	}

	std::vector<partNum_t> DatabaseImpl::listParts(const boost::string_ref& key)
	{
		checkSimpleArgumentFor(key, 0);
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		return openDatabase_->listParts(key);
	}

	//-------------------------------------------------------------------------
	// Batch API methods.

	void DatabaseImpl::checkBatchArgumentFor(const IKeyProducer& batch) const
	{
		const size_t batchSize = batch.count();

		for (size_t i = 0; i < batchSize; ++i) {
			const size_type keySize = batch.keyAt(i).size();
			const partNum_t partNum = batch.partNumAt(i);

			RAISE_INVALID_ARGUMENT_IF(keySize > MAX_KEY_SIZE, "key at index %u too long", i);
			RAISE_INVALID_ARGUMENT_IF(keySize == 0, "empty key at index %u is not allowed", i);
			RAISE_INVALID_ARGUMENT_IF(partNum > MAX_PARTNUM, "partNum at index %u is too large", i);
			RAISE_INVALID_ARGUMENT_IF(partNum == ALL_PARTS, "partNum ALL_PARTS at index %u is not allowed");
			RAISE_INVALID_ARGUMENT_IF(partNum < 0, "negative partNum at index %u is not allowed", i);
		}
	}

	bool DatabaseImpl::fetch(IReadBatch& readBatch)
	{
		checkBatchArgumentFor(readBatch);
		return doFetch(readBatch);
	}

	void DatabaseImpl::store(const IWriteBatch& writeBatch)
	{
		checkBatchArgumentFor(writeBatch);
		doStore(writeBatch);
	}

	void DatabaseImpl::remove(const IDeleteBatch& deleteBatch)
	{
		checkBatchArgumentFor(deleteBatch);
		doRemove(deleteBatch);
	}

	//-------------------------------------------------------------------------
	// Iterator.

	Iterator DatabaseImpl::newIterator()
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");

		Iterator iterator(new IteratorImpl(openDatabase_));
		return iterator;
	}

	//-------------------------------------------------------------------------
	// Statistics.

	Statistics DatabaseImpl::statistics()
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		return openDatabase_->statistics();
	}

	//-------------------------------------------------------------------------
	// Management of database files.

	namespace {

		struct ManagedDatabaseFiles {
			ManagedDatabaseFiles(const boost::filesystem::path& database)
				: bucketFile_(OpenFiles::databaseNameToBucketFileName(database))
				, overflowFile_(OpenFiles::databaseNameToOverflowFileName(database))
			{

			}

			const boost::filesystem::path bucketFile_;
			const boost::filesystem::path overflowFile_;
		};

		bool singleFileNotFound(const boost::filesystem::file_status& fileStatus)
		{
			return fileStatus.type() == boost::filesystem::file_not_found;
		}

		bool singleFileIsNonRegular(const boost::filesystem::file_status& fileStatus)
		{
			return fileStatus.type() != boost::filesystem::file_not_found && fileStatus.type() != boost::filesystem::regular_file;
		}

		std::string singleFileTypeString(const boost::filesystem::file_status& fileStatus)
		{
			const boost::filesystem::file_type fileType = fileStatus.type();

			switch (fileType) {
			case boost::filesystem::status_error:
				return "status_error";
			case boost::filesystem::file_not_found:
				return "file_not_found";
			case boost::filesystem::regular_file:
				return "regular_file";
			case boost::filesystem::directory_file:
				return "directory_file";
			case boost::filesystem::symlink_file:
				return "symlink_file";
			case boost::filesystem::block_file:
				return "block_file";
			case boost::filesystem::character_file:
				return "character_file";
			case boost::filesystem::fifo_file:
				return "fifo_file";
			case boost::filesystem::socket_file:
				return "socket_file";
			case boost::filesystem::reparse_file:
				return "reparse_file";
			case boost::filesystem::type_unknown:
				return "type_unknown";
			default:
				std::ostringstream os;
				os << "unknown(" << fileType << ")";
				return os.str();
			}
		}

		bool singleFileExists(const boost::filesystem::file_status& fileStatus)
		{
			return fileStatus.type() == boost::filesystem::regular_file;
		}

		bool databaseFilesExist(const ManagedDatabaseFiles& databaseFiles)
		{
			boost::system::error_code bucketStatusError;
			const boost::filesystem::file_status bucketFileStatus = boost::filesystem::status(databaseFiles.bucketFile_, bucketStatusError);

			boost::system::error_code overflowStatusError;
			const boost::filesystem::file_status overflowFileStatus = boost::filesystem::status(databaseFiles.overflowFile_, overflowStatusError);

			RAISE_IO_ERROR_IF(bucketStatusError   && ! singleFileNotFound(bucketFileStatus),   "existence of the bucket file \"%s\" cannot be determined: %s", databaseFiles.bucketFile_.string(), bucketStatusError.message());
			RAISE_IO_ERROR_IF(overflowStatusError && ! singleFileNotFound(overflowFileStatus), "existence of the overflow file \"%s\" cannot be determined: %s", databaseFiles.overflowFile_.string(), overflowStatusError.message());
			RAISE_IO_ERROR_IF(singleFileIsNonRegular(bucketFileStatus),   "bucket file \"%s\" is not a regular file: %s", databaseFiles.bucketFile_.string(), singleFileTypeString(bucketFileStatus));
			RAISE_IO_ERROR_IF(singleFileIsNonRegular(overflowFileStatus), "overflow file \"%s\" is not a regular file: %s", databaseFiles.overflowFile_.string(), singleFileTypeString(overflowFileStatus));

			return singleFileExists(bucketFileStatus) && singleFileExists(overflowFileStatus);
		}

	} // anonymous namespace

	bool DatabaseImpl::exists(const boost::filesystem::path& database)
	{
		RAISE_INVALID_ARGUMENT_IF(database.empty(), "database name is empty");
		RAISE_INVALID_ARGUMENT_IF(openDatabase_, "database is open");

		ManagedDatabaseFiles databaseFiles(database);
		return databaseFilesExist(databaseFiles);
	}

	bool DatabaseImpl::rename(const boost::filesystem::path& source, const boost::filesystem::path& target)
	{
		RAISE_INVALID_ARGUMENT_IF(source.empty(), "source database name is empty");
		RAISE_INVALID_ARGUMENT_IF(target.empty(), "target database name is empty");
		RAISE_INVALID_ARGUMENT_IF(openDatabase_, "database is open");

		ManagedDatabaseFiles sourceFiles(source);
		ManagedDatabaseFiles targetFiles(target);

		// We cannot make it atomic anyway.
		const bool canRename = databaseFilesExist(sourceFiles) && ! databaseFilesExist(targetFiles);

		if (canRename) {
			boost::system::error_code bucketRenameError;
			boost::filesystem::rename(sourceFiles.bucketFile_, targetFiles.bucketFile_, bucketRenameError);
			RAISE_IO_ERROR_IF(bucketRenameError, "bucket file \"%s\" cannot be renamed to \"%s\": %s", sourceFiles.bucketFile_.string(), targetFiles.bucketFile_.string(), bucketRenameError.message());
		
			boost::system::error_code overflowRenameError;
			boost::filesystem::rename(sourceFiles.overflowFile_, targetFiles.overflowFile_, overflowRenameError);
			RAISE_IO_ERROR_IF(overflowRenameError, "overflow file \"%s\" cannot be renamed to \"%s\": %s", sourceFiles.overflowFile_.string(), targetFiles.overflowFile_.string(), overflowRenameError.message());
		}

		return canRename;
	}

	bool DatabaseImpl::drop(const boost::filesystem::path& database)
	{
		RAISE_INVALID_ARGUMENT_IF(database.empty(), "database name is empty");
		RAISE_INVALID_ARGUMENT_IF(openDatabase_, "database is open");

		ManagedDatabaseFiles databaseFiles(database);

		boost::system::error_code bucketRemoveError;
		const bool bucketFileExisted = boost::filesystem::remove(databaseFiles.bucketFile_, bucketRemoveError);

		boost::system::error_code overflowRemoveError;
		const bool overflowFileExisted = boost::filesystem::remove(databaseFiles.overflowFile_, overflowRemoveError);

		RAISE_IO_ERROR_IF(bucketRemoveError, "bucket file \"%s\" cannot be deleted: %s", databaseFiles.bucketFile_.string(), bucketRemoveError.message());
		RAISE_IO_ERROR_IF(overflowRemoveError, "overflow file \"%s\" cannot be deleted: %s", databaseFiles.overflowFile_.string(), overflowRemoveError.message());

		return bucketFileExisted && overflowFileExisted;
	}

	//-------------------------------------------------------------------------
	// Private.

	bool DatabaseImpl::doFetch(IReadBatch& readBatch)
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		return openDatabase_->fetch(readBatch);
	}

	void DatabaseImpl::doStore(const IWriteBatch& writeBatch)
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		openDatabase_->store(writeBatch);
	}

	void DatabaseImpl::doRemove(const IDeleteBatch& deleteBatch)
	{
		RAISE_INVALID_ARGUMENT_IF(! openDatabase_, "database is not open");
		openDatabase_->remove(deleteBatch);
	}

}; // namespace hashdb
}; // namespace kerio
