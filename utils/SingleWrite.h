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

// SingleWrite.h - helper class for a single store using the batch API.
#pragma once
#include <kerio/hashdb/HashDB.h>
#include <kerio/hashdb/StringOrReference.h>

namespace kerio {
namespace hashdb {

	class SingleWrite : public IWriteBatch { // intentionally copyable
	public:
		SingleWrite(const boost::string_ref& key, partNum_t partNum, const boost::string_ref& value)
			: key_(key)
			, partNum_(partNum)
			, value_(value)
		{

		}

		virtual ~SingleWrite()
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

		virtual StringOrReference valueAt(size_t) const 
		{
			return StringOrReference::reference(value_);
		}

	private:
		const boost::string_ref key_;
		const partNum_t partNum_; 
		const boost::string_ref value_;
	};

}; // hashdb
}; // kerio
