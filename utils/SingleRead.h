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

// SingleRead.h - helper class for a single fetch using the batch API.
#pragma once
#include <kerio/hashdb/HashDB.h>
#include <kerio/hashdb/StringOrReference.h>

namespace kerio {
namespace hashdb {

	class SingleRead : public IReadBatch { // intentionally copyable
	public:
		SingleRead(const boost::string_ref& key, partNum_t partNum, std::string& value)
			: key_(key)
			, partNum_(partNum)
			, value_(value)
		{

		}

		virtual ~SingleRead()
		{  }

		virtual size_t count() const
		{
			return 1;
		}

		virtual StringOrReference keyAt(size_t) const
		{
			return StringOrReference::reference(key_);
		}

		virtual partNum_t partNumAt(size_t) const
		{
			return partNum_;
		}

		virtual bool setValueAt(size_t, const boost::string_ref& value)
		{
			value_.assign(value.begin(), value.end());
			return true;
		}

		virtual bool setLargeValueAt(size_t index, std::istream& valueStream, size_t valueSize);

	private:
		const boost::string_ref key_;
		const partNum_t partNum_; 
		std::string& value_;
	};

}; // hashdb
}; // kerio
