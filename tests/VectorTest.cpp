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

#if defined _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4724)      // potential mod by 0
#endif // defined _MSC_VER

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/algorithm/cxx11/is_sorted.hpp>

#if defined _MSC_VER
#pragma warning(pop)
#endif // defined _MSC_VER

#include "db/Vector.h"
#include "VectorTest.h"

using namespace kerio::hashdb;

void VectorTest::testReserve()
{
	static const size_type staticSize = 10;
	static const size_type maxSize = staticSize * 2;
	typedef Vector<int, staticSize> Vector_t;

	Vector_t mutableVector;

	TS_ASSERT_EQUALS(staticSize, mutableVector.capacity());
	
	mutableVector.reserve(staticSize + 1);
	TS_ASSERT_EQUALS(staticSize + 1, mutableVector.capacity());

	mutableVector.reserve(maxSize);
	TS_ASSERT_EQUALS(maxSize, mutableVector.capacity());
}

void VectorTest::testResize()
{
	static const size_type staticSize = 10;
	static const size_type maxSize = staticSize * 2;
	typedef Vector<size_type, staticSize> Vector_t;

	Vector_t mutableVector;

	// Upsize.
	for (size_type i = 0; i < maxSize; ++i) {
		mutableVector.resize(i);
		TS_ASSERT_EQUALS(i, mutableVector.size());

		for (size_type j = 0; j < i; ++j) {
			TS_ASSERT_EQUALS(0U, mutableVector[j]);
		}
	}

	// Downsize.
	for (size_type i = maxSize - 1; i > 0; --i) {
		mutableVector.resize(i);
		TS_ASSERT_EQUALS(i, mutableVector.size());

		for (size_type j = 0; j < i; ++j) {
			TS_ASSERT_EQUALS(0U, mutableVector[j]);
		}
	}
}

void VectorTest::testAddAndRead()
{
	static const size_type staticSize = 10;
	static const int maxSize = static_cast<int>(staticSize * 2);
	typedef Vector<int, staticSize> Vector_t;

	Vector_t mutableVector;

	TS_ASSERT(mutableVector.empty());
	TS_ASSERT_EQUALS(staticSize, mutableVector.capacity());

	for (int i = 0; i < maxSize; ++i) {
		mutableVector.push_back(i);

		const Vector_t constVector = mutableVector;

		TS_ASSERT(! mutableVector.empty());
		TS_ASSERT(! constVector.empty());

		TS_ASSERT_EQUALS(0, mutableVector.front());
		TS_ASSERT_EQUALS(i, mutableVector.back());

		TS_ASSERT_EQUALS(0, constVector.front());
		TS_ASSERT_EQUALS(i, constVector.back());

		for (int j = 0; j <= i; ++j) {
			TS_ASSERT_EQUALS(j, mutableVector[j]);
			TS_ASSERT_EQUALS(j, constVector[j]);
		}

		int ix = 0;
		Vector_t::const_iterator ii = constVector.begin();
		for (Vector_t::iterator jj = mutableVector.begin(); jj != mutableVector.end(); ++jj, ++ii) {
			TS_ASSERT_EQUALS(ix, *jj);
			TS_ASSERT_EQUALS(ix, *ii);
			++ix;
		}
	}
}

namespace {

	class NonCp1 {
	public:
		explicit NonCp1(int value) : value_(value) { }
		int value() { return value_; }
		NonCp1(BOOST_RV_REF(NonCp1) x) : value_(x.value_) {   }
		NonCp1& operator=(BOOST_RV_REF(NonCp1) x) { value_ = x.value_; x.value_ = -9999; return *this; }

	private:
		BOOST_MOVABLE_BUT_NOT_COPYABLE(NonCp1)
		int value_;
	};

	class NonCp2 {
	public:
		explicit NonCp2(int value1, int value2) : value1_(value1), value2_(value2) { }
		int value1() { return value1_; }
		int value2() { return value2_; }
		NonCp2(BOOST_RV_REF(NonCp2) x) : value1_(x.value1_), value2_(x.value2_) {   }
		NonCp2& operator=(BOOST_RV_REF(NonCp2) x) { value1_ = x.value1_; x.value1_ = -9999; value2_ = x.value2_; x.value2_ = -9999; return *this; }

	private:
		BOOST_MOVABLE_BUT_NOT_COPYABLE(NonCp2)
		int value1_;
		int value2_;
	};

};

void VectorTest::testEmplaceAndRead()
{
	static const size_type staticSize = 10;
	static const int maxSize = static_cast<int>(staticSize * 2);

	typedef Vector<NonCp1, staticSize> Vector1_t;
	Vector1_t mutableVector1;

	TS_ASSERT(mutableVector1.empty());
	TS_ASSERT_EQUALS(staticSize, mutableVector1.capacity());

	for (int i = 0; i < maxSize; ++i) {
		NonCp1& cp = mutableVector1.emplace_back(i);

		TS_ASSERT(! mutableVector1.empty());
		TS_ASSERT_EQUALS(0, mutableVector1.front().value());
		TS_ASSERT_EQUALS(i, mutableVector1.back().value());
		TS_ASSERT_EQUALS(i, cp.value());

		for (int j = 0; j <= i; ++j) {
			TS_ASSERT_EQUALS(j, mutableVector1[j].value());
		}

		int ix = 0;
		for (Vector1_t::iterator jj = mutableVector1.begin(); jj != mutableVector1.end(); ++jj) {
			TS_ASSERT_EQUALS(ix, jj->value());
			++ix;
		}
	}

	typedef Vector<NonCp2, staticSize> Vector2_t;
	Vector2_t mutableVector2;

	TS_ASSERT(mutableVector2.empty());
	TS_ASSERT_EQUALS(staticSize, mutableVector2.capacity());

	for (int i = 0; i < maxSize; ++i) {
		NonCp2& cp = mutableVector2.emplace_back(i, i+66);

		TS_ASSERT(! mutableVector2.empty());
		TS_ASSERT_EQUALS(0, mutableVector2.front().value1());
		TS_ASSERT_EQUALS(66, mutableVector2.front().value2());
		TS_ASSERT_EQUALS(i, mutableVector2.back().value1());
		TS_ASSERT_EQUALS(i+66, mutableVector2.back().value2());
		TS_ASSERT_EQUALS(i, cp.value1());
		TS_ASSERT_EQUALS(i+66, cp.value2());

		for (int j = 0; j <= i; ++j) {
			TS_ASSERT_EQUALS(j, mutableVector2[j].value1());
			TS_ASSERT_EQUALS(j+66, mutableVector2[j].value2());
		}

		int ix = 0;
		for (Vector2_t::iterator jj = mutableVector2.begin(); jj != mutableVector2.end(); ++jj) {
			TS_ASSERT_EQUALS(ix, jj->value1());
			TS_ASSERT_EQUALS(ix+66, jj->value2());
			++ix;
		}
	}
}

void VectorTest::testAddModify()
{
	static const size_type staticSize = 10;
	static const int maxSize = static_cast<int>(staticSize * 2);
	typedef Vector<int, staticSize> Vector_t;
	static const int bigIntValue = 0xfffffff;

	Vector_t mutableVector;

	TS_ASSERT(mutableVector.empty());
	TS_ASSERT_EQUALS(staticSize, mutableVector.capacity());

	// Add and test.
	for (int i = 0; i < maxSize; ++i) {
		mutableVector.push_back(i - 111);

		TS_ASSERT(! mutableVector.empty());
		TS_ASSERT_EQUALS(-111, mutableVector.front());
		TS_ASSERT_EQUALS(i - 111, mutableVector.back());

		for (int j = 0; j <= i; ++j) {
			TS_ASSERT_EQUALS(j - 111, mutableVector[j]);
		}
	}

	// Modify via operator[].
	for (int i = 0; i < maxSize; ++i) {
		mutableVector[i] = i;

		TS_ASSERT_EQUALS(0, mutableVector.front());

		Vector_t::iterator rv = mutableVector.begin();
		for (int j = 0; j <= i; ++j, ++rv) {
			TS_ASSERT_EQUALS(j, mutableVector[j]);
			TS_ASSERT_EQUALS(j, *rv);
		}
	}

	// Modify via iterator.
	int assignVal = bigIntValue;
	for (Vector_t::iterator ii = mutableVector.begin(); ii != mutableVector.end(); ++ii) {
		*ii = assignVal;
		assignVal /= 2;
	}

	TS_ASSERT_EQUALS(bigIntValue, mutableVector.front());

	// Validate via iterator.
	int checkVal = bigIntValue;
	for (Vector_t::iterator ii = mutableVector.begin(); ii != mutableVector.end(); ++ii) {
		TS_ASSERT_EQUALS(checkVal, *ii);
		checkVal /= 2;
	}

	mutableVector.clear();
	TS_ASSERT(mutableVector.empty());
}

void VectorTest::testFind()
{
	static const size_type vectorSize = 10;
	static const int maxInts = static_cast<int>(vectorSize * 2);
	typedef Vector<int, vectorSize> Vector_t;

	Vector_t mutableVector;

	// Add and test.
	for (int i = 0; i < maxInts; ++i) {
		mutableVector.push_back(i);
	}

	for (int i = 0; i < maxInts; ++i) {
		TS_ASSERT_EQUALS(static_cast<size_type>(i), mutableVector.find(i));
		TS_ASSERT(mutableVector.contains(i));
	}

	TS_ASSERT_EQUALS(Vector_t::npos, mutableVector.find(-1));
	TS_ASSERT(! mutableVector.contains(-1));
}

void VectorTest::testSmallSort()
{
	static const size_type vectorStaticSize = 10;
	static const int maxInts = static_cast<int>(vectorStaticSize * 2);
	typedef Vector<int, vectorStaticSize> Vector_t;

	Vector_t mutableVector;

	// Add, sort and test.
	for (int i = 0; i < maxInts; ++i) {
		mutableVector.push_back(maxInts - 1 - i);
	}

	std::sort(mutableVector.begin(), mutableVector.end());

	mutableVector.sort();

	for (int i = 0; i < maxInts; ++i) {
		TS_ASSERT_EQUALS(i, mutableVector[i]);
	}
}

void VectorTest::testLargeSort()
{
	static const size_type vectorStaticSize = 1000;
	static const int maxInts = static_cast<int>(vectorStaticSize * 2);
	typedef Vector<int, vectorStaticSize> Vector_t;

	Vector_t mutableVector;

	// Add, sort and test.
	boost::random::mt19937 rng(12345);
	boost::random::uniform_int_distribution<int> dist(-1000, 1000);

	for (int i = 0; i < maxInts; ++i) {
		mutableVector.push_back(dist(rng));
	}

	std::sort(mutableVector.begin(), mutableVector.end());

	mutableVector.sort();
	const bool isSorted = boost::algorithm::is_sorted(mutableVector.begin(), mutableVector.end());
	TS_ASSERT(isSorted);
}

void VectorTest::testIterator()
{
	typedef Vector<int> Vector_t;
	const int ELEMENTS = static_cast<int>(2 * Vector_t::static_capacity);

	Vector_t mutableVector;

	for (int i = 0; i < ELEMENTS; ++i) {
		mutableVector.push_back(i);
	}

	// operator!=()
	TS_ASSERT(mutableVector.begin() != mutableVector.end());

	// Prefix inc/dec operators.
	Vector_t::iterator prefixIncIterator = mutableVector.begin();
	for (int i = 0; i < ELEMENTS; ++i) {
		TS_ASSERT_EQUALS(i, *prefixIncIterator);
		
		Vector_t::iterator previous = prefixIncIterator;
		Vector_t::iterator next = ++prefixIncIterator;
		TS_ASSERT_EQUALS(previous, --next);
	}
	TS_ASSERT_EQUALS(prefixIncIterator, mutableVector.end());

	// Postfix inc/dec operators.
	Vector_t::iterator postfixIncIterator = mutableVector.begin();
	for (int i = 0; i < ELEMENTS; ++i) {
		Vector_t::iterator previous = postfixIncIterator;
		
		TS_ASSERT_EQUALS(i, *postfixIncIterator++);
		
		Vector_t::iterator next = postfixIncIterator;
		TS_ASSERT_EQUALS(postfixIncIterator, next--);
		TS_ASSERT_EQUALS(previous, next);
	}
	TS_ASSERT_EQUALS(postfixIncIterator, mutableVector.end());

	// VectorIterator& operator+=(size_t increment)
	Vector_t::iterator plusAssign = mutableVector.begin();
	plusAssign += 5;
	TS_ASSERT_EQUALS(5, *plusAssign);

	// VectorIterator operator+(size_t increment) const
	Vector_t::iterator add = mutableVector.begin() + 3;
	TS_ASSERT_EQUALS(3, *add);

	// VectorIterator& operator-=(size_t decrement)
	Vector_t::iterator minusAssign = mutableVector.begin() + 7;
	minusAssign -= 3;
	TS_ASSERT_EQUALS(4, *minusAssign);

	// VectorIterator operator-(size_t decrement) const
	Vector_t::iterator minus = mutableVector.begin() + 3;
	minus = minus - 3;
	TS_ASSERT_EQUALS(0, *minus);

	// difference_type operator-(const VectorIterator& other) const
	Vector_t::iterator diff = mutableVector.begin() + 3;
	TS_ASSERT_EQUALS(3, diff - mutableVector.begin());

	// reference operator[](size_t offset) const
	TS_ASSERT_EQUALS(8, mutableVector.begin()[8]);
}
