#include "stdafx.h"
#include <kerio/hashdb/HashDB.h>
#include <kerio/hashdb/Exception.h>
#include "utils/MurmurHash3.h"
#include "utils/MurmurHash3Adapter.h"
#include "MurmurHash3Test.h"

using namespace kerio::hashdb;

//-----------------------------------------------------------------------------
// This should hopefully be a thorough and uambiguous test of whether a hash
// is correctly implemented on a given platform

// Test adopted from https://code.google.com/p/smhasher/wiki/MurmurHash3 file KeysetTest.cpp.

typedef void (*pfHash) ( const void * blob, const int len, const uint32_t seed, void * out );

void VerificationTest (pfHash hash, const int hashbits, uint32_t expected)
{
	const int hashbytes = hashbits / 8;

	uint8_t * key    = new uint8_t[256];
	uint8_t * hashes = new uint8_t[hashbytes * 256];
	uint8_t * final  = new uint8_t[hashbytes];

	memset(key,0,256);
	memset(hashes,0,hashbytes*256);
	memset(final,0,hashbytes);

	// Hash keys of the form {0}, {0,1}, {0,1,2}... up to N=255,using 256-N as
	// the seed

	for(int i = 0; i < 256; i++)
	{
		key[i] = (uint8_t)i;

		hash(key,i,256-i,&hashes[i*hashbytes]);
	}

	// Then hash the result array

	hash(hashes,hashbytes*256,0,final);

	// The first four bytes of that hash, interpreted as a little-endian integer, is our
	// verification value

	uint32_t verification = (final[0] << 0) | (final[1] << 8) | (final[2] << 16) | (final[3] << 24);

	delete [] key;
	delete [] hashes;
	delete [] final;

	//----------

	TS_ASSERT_EQUALS(expected, verification);
}

void MurmurHash3Test::testValidate32bitHash()
{
	VerificationTest(MurmurHash3_x86_32, 32, 0xB0F57EE3);
}

void MurmurHash3Test::testValidate128bitHash_x86()
{
	VerificationTest(MurmurHash3_x86_128, 128, 0xB3ECE62A);
}

void MurmurHash3Test::testValidate128bitHash_x64()
{
	VerificationTest(MurmurHash3_x64_128, 128, 0x6384BA69);
}

void MurmurHash3Test::testAdapter()
{
	const char testValue1[] = "";
	const uint32_t result1 = kerio::hashdb::murmur3Hash(testValue1, sizeof(testValue1));
	TS_ASSERT_EQUALS(0x514E28B7U, result1);

	const char testValue2[] = "ZaUXuk&jZC1_KUZkSrXsIvOA";
	const uint32_t result2 = kerio::hashdb::murmur3Hash(testValue2, sizeof(testValue2));
	TS_ASSERT_EQUALS(0xE7F2C661U, result2);
}
