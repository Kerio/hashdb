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

// StringOrReference.h - a sequence of bytes optionally managed by the class.
#pragma once
#include <string>
#include <boost/utility/string_ref.hpp>
#include <boost/variant.hpp>
#include <kerio/hashdb/Types.h>

namespace kerio {
namespace hashdb {

	class StringOrReference  { // intentionally copyable
	public:
		boost::string_ref getRef() const
		{
			const boost::string_ref* myRef = boost::get<const boost::string_ref>(&value_);
			return (myRef != NULL)? *myRef : *boost::get<const std::string>(&value_);
		}

		size_type size() const
		{
			return static_cast<size_type>(getRef().size());
		}

		int which() const
		{
			return value_.which();
		}

		// Copy the value.
		static StringOrReference copy(const std::string& str)
		{
			return StringOrReference(str);
		}

#if !defined( BOOST_NO_CXX11_RVALUE_REFERENCES )
        static StringOrReference copy(std::string &&str)
        {
            return StringOrReference(std::move(str));
        }
#endif

		// Reference the value.
		static StringOrReference reference(const boost::string_ref& ref)
		{
			return StringOrReference(ref);
		}

	private:
		const boost::variant<const boost::string_ref, const std::string> value_;

		explicit StringOrReference(const std::string& str) : value_(str) { }

#if !defined( BOOST_NO_CXX11_RVALUE_REFERENCES )
        explicit StringOrReference(std::string &&str) : value_(std::move(str)) { }
#endif

        explicit StringOrReference(const boost::string_ref& ref) : value_(ref) { }
	};

}; // hashdb
}; // kerio
