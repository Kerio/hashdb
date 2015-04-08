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
#include <kerio/hashdb/HashDB.h>
#include "utils/SingleWrite.h"
#include "SingleWriteHelperTest.h"

using namespace kerio::hashdb;

void SingleWriteHelperTest::testSingleWriteHelper()
{
	const boost::string_ref expectedKey("TTHyRxwZw7nbrTL1g3\005L5rl9mSE24G2FUA0RSPjKrA\1");
	const partNum_t expectedPartNum = 13;
	const boost::string_ref expectedValue("l42OJZmo52O7jkRdaSPwGHledilbvs7Vkfi7nEE/oM0NIrZHBnxgiHWKEw==");

	boost::scoped_ptr<IWriteBatch> singleWrite(new SingleWrite(expectedKey, expectedPartNum, expectedValue));
	TS_ASSERT_EQUALS(singleWrite->count(), 1U);
	TS_ASSERT_EQUALS(singleWrite->keyAt(0).getRef(), expectedKey);
	TS_ASSERT_EQUALS(singleWrite->partNumAt(0), expectedPartNum);
	TS_ASSERT_EQUALS(singleWrite->valueAt(0).getRef(), expectedValue);
}
