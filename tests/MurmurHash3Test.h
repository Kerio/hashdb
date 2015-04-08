#pragma once

class MurmurHash3Test : public CxxTest::TestSuite {
public:
	void testValidate32bitHash();
	void testValidate128bitHash_x86();
	void testValidate128bitHash_x64();
	void testAdapter();
};
