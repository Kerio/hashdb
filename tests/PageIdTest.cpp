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
#include <kerio/hashdb/Types.h>
#include <kerio/hashdb/Exception.h>
#include "db/PageId.h"
#include "PageIdTest.h"

using namespace kerio::hashdb;

void PageIdTest::testInvalid()
{
	PageId invalid;
	TS_ASSERT(! invalid.isValid());

	PageId valid(PageId::OverflowFileType, 55);
	TS_ASSERT(valid.isValid());
}

void PageIdTest::testCreate()
{
	PageId resource1(PageId::BucketFileType, 333);
	TS_ASSERT_EQUALS(resource1.fileType(), PageId::BucketFileType);
	TS_ASSERT_EQUALS(resource1.pageNumber(), 333U);

	PageId resource2 = bucketFilePage(123);
	TS_ASSERT_EQUALS(resource2.fileType(), PageId::BucketFileType);
	TS_ASSERT_EQUALS(resource2.pageNumber(), 123U);

	PageId resource3 = overflowFilePage(555);
	TS_ASSERT_EQUALS(resource3.fileType(), PageId::OverflowFileType);
	TS_ASSERT_EQUALS(resource3.pageNumber(), 555U);
}

void PageIdTest::testEquals()
{
	PageId bucketPage0a = bucketFilePage(0);
	PageId bucketPage0b = bucketFilePage(0);
	PageId bucketPage1 = bucketFilePage(1);
	PageId overflowPage0 = overflowFilePage(0);

	TS_ASSERT(bucketPage0a == bucketPage0b);
	TS_ASSERT(bucketPage0a != bucketPage1);
	TS_ASSERT(bucketPage0a != overflowPage0);
}

void PageIdTest::testLessThan()
{
	PageId bucketPage0a = bucketFilePage(0);
	PageId bucketPage0b = bucketFilePage(0);
	PageId bucketPage1 = bucketFilePage(1);
	PageId overflowPage0 = overflowFilePage(0);

	// ">"
	TS_ASSERT(! (bucketPage0a > bucketPage0b));
	TS_ASSERT(! (bucketPage0a > bucketPage1));
	TS_ASSERT(bucketPage1 > bucketPage0a);
	TS_ASSERT(overflowPage0 > bucketPage1);

	// ">="
	TS_ASSERT(bucketPage0a >= bucketPage0b);
	TS_ASSERT(! (bucketPage0a >= bucketPage1));
	TS_ASSERT(bucketPage1 >= bucketPage0a);
	TS_ASSERT(overflowPage0 >= bucketPage1);

	// "<"
	TS_ASSERT(! (bucketPage0b < bucketPage0a));
	TS_ASSERT(! (bucketPage1 < bucketPage0a));
	TS_ASSERT(bucketPage0a < bucketPage1);
	TS_ASSERT(bucketPage1 < overflowPage0);

	// "<="
	TS_ASSERT(bucketPage0b <= bucketPage0a);
	TS_ASSERT(! (bucketPage1 <= bucketPage0a));
	TS_ASSERT(bucketPage0a <= bucketPage1);
	TS_ASSERT(bucketPage1 <= overflowPage0);
}
