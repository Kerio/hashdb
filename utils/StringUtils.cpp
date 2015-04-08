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

// StringUtils.cpp - string utilities.
#include "stdafx.h"
#include <kerio/hashdb/Constants.h>
#include "StringUtils.h"

namespace kerio {
namespace hashdb {

	std::string formatMessage(const char *fmt)
	{
		return fmt;
	}

	void printJsonString(std::ostream& os, const boost::string_ref& s)
	{
		for (boost::string_ref::const_iterator ii = s.begin(); ii != s.end(); ++ii) {
			const std::string::value_type element = *ii;

			if (element == '\"') { // Escape double quote.
				os << "\\\"";
			}
			else if (element == '\\') { // Escape backslash.
				os << "\\\\";
			}
			else if (element == '\n') { // Escape new line.
				os << "\\n";
			}
			else if (element == '\r') { // Escape carriage return.
				os << "\\r";
			}
			else if (element == '\t') { // Escape tab.
				os << "\\t";
			}
			else if ((element < ' ') || (element >= 127)) { // Escape non-printables.
				os << formatMessage("\\u%04x", static_cast<uint16_t>(element) & 0xff);
			}
			else {
				os << element;
			}
		}
	}

	void printJsonNameValue(std::ostream& os, const boost::string_ref& name, const boost::string_ref& value)
	{
		os << "\"" << name << "\" : \"";
		printJsonString(os, value);
		os << "\"";
	}

	void printJsonNameValue(std::ostream& os, const boost::string_ref& name, int value)
	{
		os << "\"" << name << "\" : " << value;
	}

}; // namespace hashdb
}; // namespace kerio
