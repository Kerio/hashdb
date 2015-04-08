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
#include "stdafx.h"

#if defined _WIN32
#define __DBINTERFACE_PRIVATE
#define KERIO_WIN32
#endif

#include <sstream>
#include <kerio/hashdb/Types.h>
#include <kerio/hashdb/Exception.h>
#include <bdb/db.h>
#include <fcntl.h>
#include "utils/ExceptionCreator.h"
#include "utils/ConfigUtils.h"
#include "BenchmarkOptions.h"
#include "BenchmarkException.h"
#include "HashdbWrapper.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	namespace {

		std::string keyToString(const std::string key)
		{
			std::ostringstream os;

			os << "key=\"";
			printJsonString(os, key);
			os <<"\", key size = " << key.size() << " bytes";

			return os.str();
		}

	};

	class BdbWrapperBase : public IDatabaseWrapper {
	public:
		BdbWrapperBase()
			: db_(NULL)
		{

		}

		virtual void checkedFetch(size_type number, const std::string& key, const std::string& expectedValue)
		{
			DBT keyDBT;
			keyDBT.data = (void*)key.c_str();
			keyDBT.size = key.size();

			DBT retrievedValueDBT;

			if (db_->get(db_, &keyDBT, &retrievedValueDBT, 0) == -1) {
				RAISE_BENCHMARK_EXCEPTION("db_->get failed (number=%u, %s): error=%u", number, keyToString(key), errno);
			}

			boost::string_ref actualValue(static_cast<char*>(retrievedValueDBT.data), retrievedValueDBT.size);

			if (expectedValue != actualValue) {
				RAISE_BENCHMARK_EXCEPTION("unexpected value for key number %u", number);
			}
		}

		virtual	void store(const std::string& key, const std::string& value)
		{
			DBT keyDBT;
			keyDBT.data = (void*)key.c_str();
			keyDBT.size = key.size();

			DBT valueDBT;
			valueDBT.data = (void*)value.c_str();
			valueDBT.size = value.size();

			if (db_->put(db_, &keyDBT, &valueDBT, 0) == -1) {
				std::ostringstream os;
				printJsonString(os, key);

				RAISE_BENCHMARK_EXCEPTION("db_->put failed (%s, value size = %u bytes): error=%u", keyToString(key), value.size(), errno);
			}
		}

		virtual void remove(const std::string& key)
		{
			DBT keyDBT;
			keyDBT.data = (void*)key.c_str();
			keyDBT.size = key.size();

			if (db_->del(db_, &keyDBT, 0) == -1) {
				RAISE_BENCHMARK_EXCEPTION("db_->del failed (%s): error=%u", keyToString(key), errno);
			}
		}

		virtual void close()
		{
			if (db_->close(db_) == -1) {
				RAISE_BENCHMARK_EXCEPTION("db_->close failed: error=%u", errno);
			}

			db_ = NULL;
		}

	protected:
		DB* db_;
	};

	//----------------------------------------------------------------------------

	class BdbHashWrapper : public BdbWrapperBase
	{
	public:

		BdbHashWrapper(const BenchmarkOptions& benchmarkOptions, size_type instanceNumber)
		{
			const int openFlags = (benchmarkOptions.createNew_) ? (O_RDWR | O_CREAT | O_TRUNC) : (O_RDWR);
			HASHINFO hashInfo = {
				0,					// u_int	bsize;		/* bucket size */ // default = 4K, Kerio default = 1K
				0,					// u_int	ffactor;	/* fill factor */ // 65536 = dynamic, 64 = 4K/avg size
				1,					// u_int	nelem;		/* number of elements */
				0,					// u_int	cachesize;	/* bytes to cache */
				NULL,				// u_int32_t		/* hash function */
									//		(*hash) __P((const void *, size_t));
				0					// int	lorder;		/* byte order */
			};


			if (benchmarkOptions.pageSize_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
				hashInfo.bsize = static_cast<u_int>(benchmarkOptions.pageSize_);
			}

			if (benchmarkOptions.cacheSize_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
				hashInfo.cachesize = static_cast<u_int>(benchmarkOptions.cacheSize_);
			}

			if (benchmarkOptions.initialBuckets_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
				hashInfo.nelem = benchmarkOptions.initialBuckets_;
			}

			if (benchmarkOptions.bdbHashFfactor_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
				hashInfo.ffactor = static_cast<u_int>(benchmarkOptions.bdbHashFfactor_);
			}

			std::string databaseName = benchmarkOptions.databaseName_;
			if (instanceNumber != 0) {
				databaseName += formatMessage("_%u", instanceNumber);
			}

			db_ = dbopen(databaseName.c_str(), openFlags, 0644, DB_HASH, &hashInfo);
			if (db_ == NULL) {
				RAISE_BENCHMARK_EXCEPTION("unable to open \"%s\" with open flags=0x%08x: error=%u (try to use --new)", benchmarkOptions.databaseName_, openFlags, errno);
			}
		}
	};


	class BdbHashWrapperFactory : public IDatabaseWrapperFactory {
	public:
		virtual std::string name()
		{
			return "hash";
		}

		virtual DatabaseWrapper open(const BenchmarkOptions& benchmarkOptions, size_type instanceNumber)
		{
			DatabaseWrapper newInstance(new BdbHashWrapper(benchmarkOptions, instanceNumber));
			return newInstance;
		}
	};

	DatabaseWrapperFactory newBdbHashWrapperFactory()
	{
		DatabaseWrapperFactory newInstance(new BdbHashWrapperFactory());
		return newInstance;
	}

	//----------------------------------------------------------------------------

	class BdbBtreeWrapper : public BdbWrapperBase
	{
	public:
		BdbBtreeWrapper(const BenchmarkOptions& benchmarkOptions, size_type instanceNumber)
		{
			const int openFlags = (benchmarkOptions.createNew_) ? (O_RDWR | O_CREAT | O_TRUNC) : (O_RDWR);
			BTREEINFO btreeInfo = {
				0,					// u_long	flags;
				0,					// u_int	cachesize;	/* bytes to cache */
				0,					// int		maxkeypage;	/* maximum keys per page */
				0,					// int		minkeypage;	/* minimum keys per page */
				0,					// u_int	psize;		/* page size */
				NULL,				// int		(*compare)	/* comparison function */
				//			__P((const DBT *, const DBT *));
				NULL,				// size_t	(*prefix)	/* prefix function */
				//			__P((const DBT *, const DBT *));
				0					// int	lorder;		/* byte order */
			};

			if (benchmarkOptions.pageSize_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
				btreeInfo.psize = static_cast<u_int>(benchmarkOptions.pageSize_);
			}

			if (benchmarkOptions.cacheSize_ != BenchmarkOptions::SIZE_VALUE_NOT_SET) {
				btreeInfo.cachesize = static_cast<u_int>(benchmarkOptions.cacheSize_);
			}

			std::string databaseName = benchmarkOptions.databaseName_;
			if (instanceNumber != 0) {
				databaseName += formatMessage("_%u", instanceNumber);
			}

			db_ = dbopen(databaseName.c_str(), openFlags, 0644, DB_BTREE, &btreeInfo);
			if (db_ == NULL) {
				RAISE_BENCHMARK_EXCEPTION("unable to open \"%s\" with open flags=0x%08x: error=%u (try to use --new)", benchmarkOptions.databaseName_, openFlags, errno);
			}
		}
	};


	class BdbBtreeWrapperFactory : public IDatabaseWrapperFactory {
	public:
		virtual std::string name()
		{
			return "btree";
		}

		virtual DatabaseWrapper open(const BenchmarkOptions& benchmarkOptions, size_type instanceNumber)
		{
			DatabaseWrapper newInstance(new BdbBtreeWrapper(benchmarkOptions, instanceNumber));
			return newInstance;
		}
	};

	DatabaseWrapperFactory newBdbBtreeWrapperFactory()
	{
		DatabaseWrapperFactory newInstance(new BdbBtreeWrapperFactory());
		return newInstance;
	}

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
