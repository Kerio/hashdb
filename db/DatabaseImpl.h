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

// DatabaseImpl.h - implementation of the IDatabase interface.
#pragma once
#include <kerio/hashdb/HashDB.h>
#include "OpenDatabase.h"

namespace kerio {
namespace hashdb {

	class DatabaseImpl : public IDatabase, boost::noncopyable
	{
	public:
		DatabaseImpl();
		virtual ~DatabaseImpl();

		virtual void open(const boost::filesystem::path& database, const Options& options);
		virtual void close();
		virtual void flush();
		virtual void sync();
		virtual void releaseSomeResources();
		virtual void prefetch();

		void checkSimpleArgumentFor(const boost::string_ref& key, partNum_t partNum) const;
		virtual bool fetch(const boost::string_ref& key, partNum_t partNum, std::string& value);
		virtual	void store(const boost::string_ref& key, partNum_t partNum, const boost::string_ref& value);
		virtual void remove(const boost::string_ref& key, partNum_t partNum);
		virtual void remove(const boost::string_ref& key);
		virtual std::vector<partNum_t> listParts(const boost::string_ref& key);

		void checkBatchArgumentFor(const IKeyProducer& batch) const;
		virtual bool fetch(IReadBatch& readBatch);
		virtual void store(const IWriteBatch& writeBatch);
		virtual void remove(const IDeleteBatch& deleteBatch);

		virtual Iterator newIterator();

		virtual Statistics statistics();

		virtual bool exists(const boost::filesystem::path& database);
		virtual bool rename(const boost::filesystem::path& source, const boost::filesystem::path& target);
		virtual bool drop(const boost::filesystem::path& database);

	private:
		bool doFetch(IReadBatch& readBatch);
		void doStore(const IWriteBatch& writeBatch);
		void doRemove(const IDeleteBatch& deleteBatch);

		boost::shared_ptr<OpenDatabase> openDatabase_;
	};

}; // namespace hashdb
}; // namespace kerio
