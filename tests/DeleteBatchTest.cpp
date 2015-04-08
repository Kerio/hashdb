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
#include <kerio/hashdb/Constants.h>
#include <kerio/hashdbHelpers/DeleteBatch.h>
#include "DeleteBatchTest.h"

using namespace kerio::hashdb;

void DeleteBatchTest::testDeleteBatch()
{
	const std::string expectedKey1("TTHyRxwZw7nbrTL1g3\005L5rl9mSE24G2FUA0RSPjKrA\1");
	const partNum_t expectedPartNum1 = 13;

	const std::string expectedKey2("ad;skd;d;lqkdlqd;cdl;ckd;lc;l;kd;lcfkd");
	const partNum_t expectedPartNum2 = ALL_PARTS;

	boost::scoped_ptr<DeleteBatch> deleteBatch(new DeleteBatch);
	deleteBatch->add(expectedKey1, expectedPartNum1);
	deleteBatch->add(expectedKey2, expectedPartNum2);

	TS_ASSERT_EQUALS(deleteBatch->count(), 2U);

	TS_ASSERT_EQUALS(deleteBatch->keyAt(0).getRef(), expectedKey1);
	TS_ASSERT_EQUALS(deleteBatch->partNumAt(0), expectedPartNum1);

	TS_ASSERT_EQUALS(deleteBatch->keyAt(1).getRef(), expectedKey2);
	TS_ASSERT_EQUALS(deleteBatch->partNumAt(1), expectedPartNum2);

	deleteBatch->clear();
	TS_ASSERT_EQUALS(deleteBatch->count(), 0U);
}
