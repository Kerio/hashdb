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

// StringUtils.h - string utilities.
#pragma once
#include <stdarg.h>
#include <boost/format.hpp>
#include <kerio/hashdb/Types.h>

namespace kerio {
namespace hashdb {

	std::string formatMessage(const char *fmt);
	
	template <typename T1>
	std::string formatMessage(const char *fmt, const T1& arg1)
	{
		boost::format formatter(fmt);
		formatter % arg1;
		return formatter.str();
	}

	template <typename T1, typename T2>
	std::string formatMessage(const char *fmt, const T1& arg1, const T2& arg2)
	{
		boost::format formatter(fmt);
		formatter % arg1 % arg2;
		return formatter.str();
	}

	template <typename T1, typename T2, typename T3>
	std::string formatMessage(const char *fmt, const T1& arg1, const T2& arg2, const T3& arg3)
	{
		boost::format formatter(fmt);
		formatter % arg1 % arg2 % arg3;
		return formatter.str();
	}

	template <typename T1, typename T2, typename T3, typename T4>
	std::string formatMessage(const char *fmt, const T1& arg1, const T2& arg2, const T3& arg3, const T4& arg4)
	{
		boost::format formatter(fmt);
		formatter % arg1 % arg2 % arg3 % arg4;
		return formatter.str();
	}

	template <typename T1, typename T2, typename T3, typename T4, typename T5>
	std::string formatMessage(const char *fmt, const T1& arg1, const T2& arg2, const T3& arg3, const T4& arg4, const T5& arg5)
	{
		boost::format formatter(fmt);
		formatter % arg1 % arg2 % arg3 % arg4 % arg5;
		return formatter.str();
	}

	template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
	std::string formatMessage(const char *fmt, const T1& arg1, const T2& arg2, const T3& arg3, const T4& arg4, const T5& arg5, const T6& arg6)
	{
		boost::format formatter(fmt);
		formatter % arg1 % arg2 % arg3 % arg4 % arg5 % arg6;
		return formatter.str();
	}

	template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7>
	std::string formatMessage(const char *fmt, const T1& arg1, const T2& arg2, const T3& arg3, const T4& arg4, const T5& arg5, const T6& arg6, const T7& arg7)
	{
		boost::format formatter(fmt);
		formatter % arg1 % arg2 % arg3 % arg4 % arg5 % arg6 % arg7;
		return formatter.str();
	}

	template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7, typename T8>
	std::string formatMessage(const char *fmt, const T1& arg1, const T2& arg2, const T3& arg3, const T4& arg4, const T5& arg5, const T6& arg6, const T7& arg7, const T8& arg8)
	{
		boost::format formatter(fmt);
		formatter % arg1 % arg2 % arg3 % arg4 % arg5 % arg6 % arg7 % arg8;
		return formatter.str();
	}

	void printJsonString(std::ostream& os, const boost::string_ref& s);
	void printJsonNameValue(std::ostream& os, const boost::string_ref& name, const boost::string_ref& value);
	void printJsonNameValue(std::ostream& os, const boost::string_ref& name, int value);

}; // namespace hashdb
}; // namespace kerio
