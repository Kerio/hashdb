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
#include <kerio/hashdbHelpers/StringReadBatch.h>
#include "StringReadBatchTest.h"
#include "testUtils/StringUtils.h"

using namespace kerio::hashdb;

void StringReadBatchTest::testSingleRead()
{
	const std::string expectedKey("JHD38xjcd9");
	const partNum_t expectedPartNum = 123;
	const std::string expectedValue("Aldq30	r30edjk39dmd3d23d d32-23d\\");

	boost::scoped_ptr<StringReadBatch> singleRead(new StringReadBatch);
	singleRead->add(expectedKey, expectedPartNum);

	TS_ASSERT_EQUALS(singleRead->count(), 1U);
	TS_ASSERT_EQUALS(singleRead->keyAt(0).getRef(), expectedKey);
	TS_ASSERT_EQUALS(singleRead->partNumAt(0), expectedPartNum);

	TS_ASSERT(singleRead->setValueAt(0, expectedValue));

	TS_ASSERT_EQUALS(singleRead->count(), 1U);
	TS_ASSERT_EQUALS(singleRead->keyAt(0).getRef(), expectedKey);
	TS_ASSERT_EQUALS(singleRead->partNumAt(0), expectedPartNum);
	TS_ASSERT_EQUALS(singleRead->resultAt(0), expectedValue);

	singleRead->clear();
	TS_ASSERT_EQUALS(singleRead->count(), 0U);
}

void StringReadBatchTest::testSingleReadLargeValue()
{
	const std::string expectedKey("JHD38xjcd9");
	const partNum_t expectedPartNum = 123;
	const std::string expectedValue = valueOfSize(64 * 1024 + 13);

	boost::scoped_ptr<StringReadBatch> singleRead(new StringReadBatch);
	singleRead->add(expectedKey, expectedPartNum);

	TS_ASSERT_EQUALS(singleRead->count(), 1U);
	TS_ASSERT_EQUALS(singleRead->keyAt(0).getRef(), expectedKey);
	TS_ASSERT_EQUALS(singleRead->partNumAt(0), expectedPartNum);

	std::istringstream is(expectedValue);
	TS_ASSERT(singleRead->setLargeValueAt(0, is, expectedValue.size()));

	TS_ASSERT_EQUALS(singleRead->count(), 1U);
	TS_ASSERT_EQUALS(singleRead->keyAt(0).getRef(), expectedKey);
	TS_ASSERT_EQUALS(singleRead->partNumAt(0), expectedPartNum);
	TS_ASSERT_EQUALS(singleRead->resultAt(0), expectedValue);
}

void StringReadBatchTest::testMultipleReads()
{
	const std::string expectedKey1("JHD38xjcd9");
	const partNum_t expectedPartNum1 = 123;
	const std::string expectedValue1("Aldq30	r30edjk39dmd3d23d d32-23d\\");

	const std::string expectedKey2("45ef4x5g002");
	const partNum_t expectedPartNum2 = 0;
	const std::string expectedValue2("ncu9487nd43cr48c4n73489t57894rcm48mxrm48r4xr84r4m3r834xr834r834x7r3489t73489t73489xt7348978");

	boost::scoped_ptr<StringReadBatch> readBatch(new StringReadBatch);

	readBatch->add(expectedKey1, expectedPartNum1);
	readBatch->add(expectedKey2, expectedPartNum2);

	TS_ASSERT_EQUALS(readBatch->count(), 2U);

	TS_ASSERT_EQUALS(readBatch->keyAt(0).getRef(), expectedKey1);
	TS_ASSERT_EQUALS(readBatch->partNumAt(0), expectedPartNum1);
	TS_ASSERT(readBatch->setValueAt(0, expectedValue1));

	TS_ASSERT_EQUALS(readBatch->keyAt(1).getRef(), expectedKey2);
	TS_ASSERT_EQUALS(readBatch->partNumAt(1), expectedPartNum2);
	TS_ASSERT(readBatch->setValueAt(1, expectedValue2));

	TS_ASSERT_EQUALS(readBatch->resultAt(0), expectedValue1);
	TS_ASSERT_EQUALS(readBatch->resultAt(1), expectedValue2);

	readBatch->clear();
	TS_ASSERT_EQUALS(readBatch->count(), 0U);
}
