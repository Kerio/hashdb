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

// StringUtils.cpp - string utilities for tests.
#include "stdafx.h"
#include <limits>
#include "utils/MurmurHash3Adapter.h"
#include "StringUtils.h"

RandomChars::RandomChars(unsigned seed) : seed_(seed)
{

}

char RandomChars::next()
{
	seed_ = seed_ * 1103515245 + 12345;
	return static_cast<char>((seed_/65536) % 32768);
}

std::string randomString(size_t size, unsigned seed)
{
	std::string rv;
	rv.resize(size);

	RandomChars rnd(seed);
	for (size_t i = 0; i < size; ++i) {
		rv[i] = rnd.next();
	}

	return rv;
}

std::string sequenceString(size_t size, char first)
{
	std::string rv;
	rv.resize(size);

	for (size_t i = 0; i < size; ++i) {
		rv[i] = static_cast<std::string::value_type>(i + first);
	}

	return rv;
}

std::string keyOfSize(size_t n, unsigned seed /* = 43515 */ )
{
	return randomString(n, seed);
}

std::string valueOfSize(size_t n, unsigned seed /* = 30472 */ )
{
	return randomString(n, seed);
}

std::string keyFor(uint32_t n)
{
	static const uint32_t base = 36;
	static const char digits[] = "0123456789abcdefghijklmnopqrstuvwxyz";
		
	std::string rv;

	do {
		rv += digits[n % base];
		n /= base;
	} while (n != 0);

	std::reverse(rv.begin(), rv.end());
	return rv;
}

namespace {

	uint32_t nBits(unsigned n) 
	{
		uint32_t mask = 0;

		while (n--) {
			mask = (mask << 1) | 1;
		}

		return mask;
	}

	unsigned highestBit(uint32_t value)
	{
		unsigned bitNumber = 0;

		while (value >>= 1) {
			++bitNumber;
		}

		return bitNumber;
	}

	uint32_t bucketForKey(unsigned buckets, const boost::string_ref& key)
	{
		const uint32_t highestBucket = buckets - 1;
		const unsigned maskBits = highestBit(highestBucket) + 1;

		const uint32_t highMask = nBits(maskBits);
		const uint32_t lowMask = nBits(maskBits - 1);

		const uint32_t hash = kerio::hashdb::murmur3Hash(key.data(), key.size());
		uint32_t bucket = hash & highMask;

		if (bucket > highestBucket) {
			bucket &= lowMask;
		}

		return bucket;
	}

};

std::vector<std::string> nonCollidingKeys(unsigned number)
{
	std::vector<std::string> rv;

	for (uint32_t i = 0; i < number; ++i) {
		
		for (uint32_t j = 0; j < std::numeric_limits<uint32_t>::max(); ++j) {
			std::string possibleKey = keyFor(j);
			if (bucketForKey(number, possibleKey) == i) {
				rv.push_back(possibleKey);
				break;
			}
		}
	}

	return rv;
}
