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

// StringCopyDeleteBatch - delete batch which provides copy of the key instead of a reference.
#pragma once
#include <kerio/hashdb/BatchApi.h>

	class StringCopyDeleteBatch : public kerio::hashdb::IDeleteBatch { // intentionally copyable
	public:

		void reserve(size_t elements)
		{
			records_.reserve(elements);
		}

		void add(const std::string& key, kerio::hashdb::partNum_t partNum)
		{
			Record newRecord(key, partNum);
			records_.push_back(newRecord);
		}

		void clear()
		{
			records_.clear();
		}

		virtual size_t count() const
		{
			return records_.size();
		}

		virtual kerio::hashdb::StringOrReference keyAt(size_t index) const
		{
			return kerio::hashdb::StringOrReference::copy(records_.at(index).key_);
		}

		virtual kerio::hashdb::partNum_t partNumAt(size_t index) const
		{
			return records_.at(index).partNum_;
		}

	private:
		struct Record {
			Record()
				: partNum_(0)
			{ }

			Record(const std::string& key, kerio::hashdb::partNum_t partNum)
				: key_(key)
				, partNum_(partNum)
			{ }

			std::string key_;
			kerio::hashdb::partNum_t partNum_;
		};

		std::vector<Record> records_;
	};
