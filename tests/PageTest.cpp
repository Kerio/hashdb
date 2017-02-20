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
#include "testUtils/TestPageAllocator.h"
#include "db/BucketDataPage.h"
#include "db/OverflowDataPage.h"
#include "db/Vector.h"
#include "PageTest.h"

using namespace kerio::hashdb;

PageTest::PageTest()
	: allocator_(new TestPageAllocator())
{
	
}

void PageTest::testAlloc()
{
	{
		static const size_type PAGE_SIZE = 16;
		BucketDataPage page(allocator_.get(), PAGE_SIZE);
	}

	TS_ASSERT(allocator_->allFreed());
}

void PageTest::testEmplace()
{
	{
		static const size_type PAGE_SIZE = 16;

		Vector<OverflowDataPage, 2> entries;
		OverflowDataPage& page1 = entries.emplace_back(allocator_.get(), PAGE_SIZE);
		OverflowDataPage& page2 = entries.emplace_back(allocator_.get(), PAGE_SIZE);
		OverflowDataPage& page3 = entries.emplace_back(allocator_.get(), PAGE_SIZE);

		// Attempt to access data.
		entries[0][PAGE_SIZE - 1] = 11;
		entries[1][PAGE_SIZE - 1] = 22;
		entries[2][PAGE_SIZE - 1] = 33;

		TS_ASSERT_EQUALS(11, page1.get8(PAGE_SIZE - 1));
		TS_ASSERT_EQUALS(22, page2.get8(PAGE_SIZE - 1));
		TS_ASSERT_EQUALS(33, page3.get8(PAGE_SIZE - 1));

		TS_ASSERT_EQUALS(11, entries[0].get8(PAGE_SIZE - 1));
		TS_ASSERT_EQUALS(22, entries[1].get8(PAGE_SIZE - 1));
		TS_ASSERT_EQUALS(33, entries[2].get8(PAGE_SIZE - 1));

		TS_ASSERT(entries[0].constData() != entries[1].constData());

		entries[0].clearDirtyFlag();
		entries[1].clearDirtyFlag();
		entries[2].clearDirtyFlag();
	}

	TS_ASSERT(allocator_->allFreed());
}

void PageTest::testPutGet8()
{
	static const size_type PAGE_SIZE = 16;
	BucketDataPage page(allocator_.get(), PAGE_SIZE);

	for (size_type i = 0; i < PAGE_SIZE; ++i) {
		page[i] = static_cast<Page::value_type>(i);
		TS_ASSERT(page.dirty());
	}
	page.clearDirtyFlag();

	// get8 does not set dirty flag
	for (size_type i = 0; i < PAGE_SIZE; ++i) {
		TS_ASSERT_EQUALS(page.get8(i), i);
	}
	TS_ASSERT(! page.dirty());

	// operator[]() const does not change dirty flag
	for (size_type i = 0; i < PAGE_SIZE; ++i) {
		const Page& pageRef = page;
		TS_ASSERT_EQUALS(pageRef[i], i);
	}
	TS_ASSERT(! page.dirty());

	// Non-const operator[]() also provides the value.
	for (size_type i = 0; i < PAGE_SIZE; ++i) {
		TS_ASSERT_EQUALS(page[i], i);
	}
	TS_ASSERT(page.dirty());
	page.clearDirtyFlag();

	// Check directly.
	const Page::value_type* data = page.constData();
	for (size_type i = 0; i < PAGE_SIZE; ++i) {
		TS_ASSERT_EQUALS(data[i], i);
	}

	// Access methods should check range.
	TS_ASSERT_THROWS(page[PAGE_SIZE] = 0, InternalErrorException);
	TS_ASSERT_THROWS(page.put8(PAGE_SIZE, 0), InternalErrorException);

	TS_ASSERT_THROWS(page[PAGE_SIZE], InternalErrorException);
}

void PageTest::testPutGet16()
{
	static const size_type PAGE_SIZE = 16;
	static const uint16_t value1 = 0x1234;
	static const size_type index1 = 0;
	static const uint16_t value2 = 0x5678;
	static const size_type index2 = PAGE_SIZE - sizeof(value2);
	static const uint16_t value3 = 0xabcd;
	static const size_type index3 = sizeof(value3);

	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.clear();
	TS_ASSERT(page.dirty());
	page.clearDirtyFlag();

	page.put16(index1, value1);
	TS_ASSERT(page.dirty());
	page.put16(index2, value2);
	TS_ASSERT(page.dirty());

	page.clearDirtyFlag();
	TS_ASSERT_EQUALS(page.get16(index1), value1);
	TS_ASSERT_EQUALS(page.get16(index2), value2);

	TS_ASSERT_EQUALS(page.get16unchecked(index1), value1);
	TS_ASSERT_EQUALS(page.get16unchecked(index2), value2);
	TS_ASSERT(! page.dirty());

	// Check directly.
	const uint16_t* data = page.constData16();
	TS_ASSERT_EQUALS(data[0], value1);
	TS_ASSERT_EQUALS(data[index2 >> 1], value2);

	// Access methods should check range.
	TS_ASSERT_THROWS(page.put16(PAGE_SIZE, 0), InternalErrorException);
	TS_ASSERT_THROWS(page.get16(PAGE_SIZE), InternalErrorException);
	TS_ASSERT_THROWS(page.put16(PAGE_SIZE - 1, 0), InternalErrorException);
	TS_ASSERT_THROWS(page.get16(PAGE_SIZE - 1), InternalErrorException);

	// Unchecked access.
	page.put16unchecked(index3, value3);
	TS_ASSERT(page.dirty());
	page.clearDirtyFlag();
	TS_ASSERT_EQUALS(page.get16unchecked(index3), value3);
	TS_ASSERT(! page.dirty());
}

void PageTest::testPutGet32()
{
	static const size_type PAGE_SIZE = 16;
	static const uint32_t value1 = 0x12345878;
	static const size_type index1 = 0;
	static const uint32_t value2 = 0x87654321;
	static const size_type index2 = PAGE_SIZE - sizeof(value2);
	static const uint32_t value3 = 0x0a0b0c0d;
	static const size_type index3 = sizeof(value3);

	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.clear();
	TS_ASSERT(page.dirty());
	page.clearDirtyFlag();

	page.put32(index1, value1);
	TS_ASSERT(page.dirty());
	page.put32(index2, value2);
	TS_ASSERT(page.dirty());

	page.clearDirtyFlag();
	TS_ASSERT_EQUALS(page.get32(index1), value1);
	TS_ASSERT_EQUALS(page.get32(index2), value2);

	TS_ASSERT_EQUALS(page.get32unchecked(index1), value1);
	TS_ASSERT_EQUALS(page.get32unchecked(index2), value2);
	TS_ASSERT(! page.dirty());

	// Check directly.
	const uint32_t* data = page.constData32();
	TS_ASSERT_EQUALS(data[0], value1);
	TS_ASSERT_EQUALS(data[index2 >> 2], value2);

	// Access methods should check range.
	TS_ASSERT_THROWS(page.put32(PAGE_SIZE, 0), InternalErrorException);
	TS_ASSERT_THROWS(page.get32(PAGE_SIZE), InternalErrorException);

	TS_ASSERT_THROWS(page.put32(PAGE_SIZE - 1, 0), InternalErrorException);
	TS_ASSERT_THROWS(page.get32(PAGE_SIZE - 1), InternalErrorException);

	TS_ASSERT_THROWS(page.put32(PAGE_SIZE - 2, 0), InternalErrorException);
	TS_ASSERT_THROWS(page.get32(PAGE_SIZE - 2), InternalErrorException);

	TS_ASSERT_THROWS(page.put32(PAGE_SIZE - 3, 0), InternalErrorException);
	TS_ASSERT_THROWS(page.get32(PAGE_SIZE - 3), InternalErrorException);

	// Unchecked access.
	page.put32unchecked(index3, value3);
	TS_ASSERT(page.dirty());
	page.clearDirtyFlag();
	TS_ASSERT_EQUALS(page.get32unchecked(index3), value3);
}

void PageTest::testPutGetBytes()
{
	static const size_type PAGE_SIZE = 16;

	boost::string_ref value1("123");
	const size_type index1 = 0;

	boost::string_ref value2("abcdef");
	const size_type index2 = PAGE_SIZE - static_cast<size_type>(value2.size());

	boost::string_ref value3("");
	const size_type index3 = 5;

	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.clear();
	page.clearDirtyFlag();

	TS_ASSERT_THROWS_NOTHING(page.putBytes(index1, value1));
	TS_ASSERT_THROWS_NOTHING(page.putBytes(index2, value2));
	TS_ASSERT_THROWS_NOTHING(page.putBytes(index3, value3));

	TS_ASSERT(page.dirty());
	page.clearDirtyFlag();

	TS_ASSERT_EQUALS(page.getBytes(index1, static_cast<size_type>(value1.size())), value1);
	TS_ASSERT_EQUALS(page.getBytes(index2, static_cast<size_type>(value2.size())), value2);
	TS_ASSERT_EQUALS(page.getBytes(index3, static_cast<size_type>(value3.size())), value3);
	TS_ASSERT(! page.dirty());

	// Check directly.
	const Page::value_type* data = page.constData();
	TS_ASSERT_SAME_DATA(data, value1.data(), static_cast<unsigned>(value1.size()));
	TS_ASSERT_SAME_DATA(data + index2, value2.data(), static_cast<unsigned>(value2.size()));
	TS_ASSERT_EQUALS(data[value1.size()], 0);

	// Access methods should check range.
	TS_ASSERT_THROWS_NOTHING(page.putBytes(0, "0123456789abcdef"));
	TS_ASSERT(page.dirty());
	
	page.clearDirtyFlag();
	TS_ASSERT_THROWS(page.putBytes(0, "0123456789abcdefg"), InternalErrorException);
	TS_ASSERT_THROWS(page.putBytes(index2 + 1, value2), InternalErrorException);
	TS_ASSERT_THROWS(page.putBytes(index2 + 2, value2), InternalErrorException);

	TS_ASSERT(! page.dirty());
}

void PageTest::testHasBytes()
{
	static const size_type PAGE_SIZE = 8;
	boost::string_ref value("12345678");

	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.clear();

	TS_ASSERT_THROWS_NOTHING(page.putBytes(0, value));
	TS_ASSERT_SAME_DATA(page.constData(), value.data(), static_cast<unsigned>(value.size()));
	page.clearDirtyFlag();

	TS_ASSERT(page.hasBytes(0, value));
	TS_ASSERT(page.hasBytes(1, value.substr(1)));
	TS_ASSERT(page.hasBytes(7, value.substr(7)));
	TS_ASSERT(! page.hasBytes(0, value.substr(7)));

	// Access methods should check range.
	TS_ASSERT_THROWS(page.hasBytes(0, "0123456789"), InternalErrorException);
	TS_ASSERT_THROWS(page.hasBytes(PAGE_SIZE, "9"), InternalErrorException);
	TS_ASSERT_THROWS_NOTHING(page.hasBytes(PAGE_SIZE, ""));
}

void PageTest::testMoveBytes()
{
	static const size_type PAGE_SIZE = 16;
	boost::string_ref value("0123456789ABCDEF");

	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.putBytes(0, value);
	TS_ASSERT_SAME_DATA(page.constData(), value.data(), static_cast<unsigned>(value.size()));
	
	page.clearDirtyFlag();
	TS_ASSERT_THROWS_NOTHING(page.moveBytes(10, 9, 6));
	TS_ASSERT(page.dirty());
	TS_ASSERT_SAME_DATA(page.constData(), "01234567899ABCDE", page.size());

	TS_ASSERT_THROWS_NOTHING(page.moveBytes(0, 1, 15));
	TS_ASSERT_SAME_DATA(page.constData(), "1234567899ABCDEE", page.size());

	// Access methods should check range.
	TS_ASSERT_THROWS(page.moveBytes(0, 0, PAGE_SIZE + 1), InternalErrorException);
	TS_ASSERT_THROWS(page.moveBytes(0, 1, PAGE_SIZE), InternalErrorException);
	TS_ASSERT_THROWS(page.moveBytes(1, 0, PAGE_SIZE), InternalErrorException);
	TS_ASSERT_THROWS(page.moveBytes(PAGE_SIZE, 0, 1), InternalErrorException);

	page.clearDirtyFlag();
}

void PageTest::testClear()
{
	static const size_type PAGE_SIZE = 16;
	boost::string_ref value("0123456789ABCDEF");

	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.putBytes(0, value);
	TS_ASSERT_SAME_DATA(page.constData(), value.data(), static_cast<unsigned>(value.size()));

	page.clearDirtyFlag();
	page.clear();
	TS_ASSERT(page.dirty());

	for (size_type i = 0; i < PAGE_SIZE; ++i) {
		TS_ASSERT_EQUALS(page[i], 0);
	}

	page.clearDirtyFlag();
}

void PageTest::testXor32()
{
	static const size_type PAGE_SIZE = 16;
	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.clear();

	page.clearDirtyFlag();
	TS_ASSERT_EQUALS(page.xor32(), 0U);
	TS_ASSERT(! page.dirty());

	page[0] = 0xaa;
	TS_ASSERT_EQUALS(page.xor32(), 0xaaU);

	page[PAGE_SIZE - 1] = 0x55;
	TS_ASSERT_EQUALS(page.xor32(), 0x550000aaU);

	page[PAGE_SIZE/2] = 0x55; // 0xaa ^ 0x55 == 0xff
	page[PAGE_SIZE/2 + 1] = 0x11;
	page[PAGE_SIZE/2 + 2] = 0x22;

	page.clearDirtyFlag();
	TS_ASSERT_EQUALS(page.xor32(), 0x552211ffU);
	TS_ASSERT(! page.dirty());
}

void PageTest::testXor32Size()
{
	static const size_type PAGE_SIZE = 16;
	BucketDataPage page(allocator_.get(), PAGE_SIZE);
	page.clear();

	TS_ASSERT_EQUALS(page.xor32(), 0U);

	page[0] = 0xaa;
	page[4] = 0xbb;
	page[11] = 0x55;
	page[12] = 0x33;
	page.clearDirtyFlag();

	TS_ASSERT_EQUALS(page.xor32(4), 0xaaU);
	TS_ASSERT_EQUALS(page.xor32(8), 0x11U);
	TS_ASSERT_EQUALS(page.xor32(12), 0x55000011U);
}
