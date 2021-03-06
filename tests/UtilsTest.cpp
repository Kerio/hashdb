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
#include <limits>
#include "utils/StringUtils.h"
#include "UtilsTest.h"

using namespace kerio::hashdb;

void UtilsTest::testToHex()
{
	uint32_t minUint = std::numeric_limits<uint32_t>::min();
	std::string minStr = "0";
	TS_ASSERT_EQUALS(minStr, toHex(minUint));

	uint32_t maxUint = std::numeric_limits<uint32_t>::max();
	std::string maxStr = "ffffffff";
	TS_ASSERT_EQUALS(maxStr, toHex(maxUint));

	uint32_t digUint = 0x89abcdef;
	std::string digStr = "89abcdef";
	TS_ASSERT_EQUALS(digStr, toHex(digUint));
}

void UtilsTest::testPrintJsonString()
{
	{
		std::ostringstream os;

		printJsonString(os, "");
		TS_ASSERT(os.str().empty());
	}

	{
		std::ostringstream os;

		printJsonString(os, "abcd\n");
		TS_ASSERT_EQUALS("abcd\\n", os.str());
	}

	{
		std::ostringstream os;

		unsigned char binValues[3] = { 5, 0xf5, 0 };
		std::string binString(reinterpret_cast<const char*>(&binValues), 3);
		printJsonString(os, binString);
		TS_ASSERT_EQUALS("\\u0005\\u00f5\\u0000", os.str());
	}
}
