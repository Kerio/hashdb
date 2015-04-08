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
#include <kerio/hashdb/Exception.h>
#include <kerio/hashdb/Constants.h>
#include "utils/ConfigUtils.h"
#include "testUtils/FileUtils.h"
#include "testUtils/StringUtils.h"
#include "testUtils/TestPageAllocator.h"
#include "testUtils/TestLogger.h"
#include "testUtils/PrintException.h"
#include "ManagementTest.h"


using namespace kerio::hashdb;

ManagementTest::ManagementTest()
	: databaseTestPath_(getTestPath())
{
	
}

void ManagementTest::setUp()
{
	removeTestDirectory();
	createTestDirectory();
}

void ManagementTest::tearDown()
{
	removeTestDirectory();
}

//-----------------------------------------------------------------------------
// Tests db->exists(database).

void ManagementTest::testExists()
{
	const std::string newDb(databaseTestPath_ + "/new");
	const std::string notADb(databaseTestPath_ + "/none");

	// Create a database and close it.
	Database db = DatabaseFactory();
	TS_ASSERT_THROWS_NOTHING(db->open(newDb, Options::readWriteSingleThreaded()));
	TS_ASSERT_THROWS_NOTHING(db->close());

	// Check that database exists.
	TS_ASSERT(db->exists(newDb));

	// Check that non-existent database does not exist.
	TS_ASSERT(! db->exists(notADb));
}

void ManagementTest::testRename()
{
	const std::string sourceDb(databaseTestPath_ + "/source");
	const std::string targetDb(databaseTestPath_ + "/target");

	// Create a database and close it.
	Database db = DatabaseFactory();
	TS_ASSERT_THROWS_NOTHING(db->open(sourceDb, Options::readWriteSingleThreaded()));
	TS_ASSERT_THROWS_NOTHING(db->close());

	// Check that source database exists and target database does not exist.
	TS_ASSERT(db->exists(sourceDb));
	TS_ASSERT(! db->exists(targetDb));

	// Rename.
	TS_ASSERT(db->rename(sourceDb, targetDb));

	// Check that source database does not exist and target database exists.
	TS_ASSERT(! db->exists(sourceDb));
	TS_ASSERT(db->exists(targetDb));

	// Rename back.
	TS_ASSERT(db->rename(targetDb, sourceDb));

	// Check that source database exists and target database does not exist.
	TS_ASSERT(db->exists(sourceDb));
	TS_ASSERT(! db->exists(targetDb));

	// Second attempt to rename fails.
	TS_ASSERT(! db->rename(targetDb, sourceDb));
	TS_ASSERT(db->exists(sourceDb));
	TS_ASSERT(! db->exists(targetDb));

	// Database can still be opened.
	TS_ASSERT_THROWS_NOTHING(db->open(sourceDb, Options::readOnlySingleThreaded()));
	TS_ASSERT_THROWS_NOTHING(db->close());
}

void ManagementTest::testDrop()
{
	const std::string newDb(databaseTestPath_ + "/new");
	const std::string notADb(databaseTestPath_ + "/none");

	// Create a database and close it.
	Database db = DatabaseFactory();
	TS_ASSERT_THROWS_NOTHING(db->open(newDb, Options::readWriteSingleThreaded()));
	TS_ASSERT_THROWS_NOTHING(db->close());

	// Check that database exists and non-existent database does not exist.
	TS_ASSERT(db->exists(newDb));
	TS_ASSERT(! db->exists(notADb));

	// Attempt to drop databases.
	TS_ASSERT(db->drop(newDb));
	TS_ASSERT(! db->drop(notADb));

	// Check that databases do not exist.
	TS_ASSERT(! db->exists(newDb));
	TS_ASSERT(! db->exists(notADb));
}
