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
#include <iostream>
#include <kerio/hashdb/Exception.h>
#include "Exception.h"
#include "CommandOptions.h"

using namespace kerio::hashdb::tool;

ReturnCode run(int argc, char** argv)
{
	ReturnCode returnCode = SuccessReturnCode;

	try {

		CommandOptions options(argc, argv);

		if (! options.quiet_) {
			std::cout << options.list() << std::endl;
		}

		options.command_->run(options);

		if (! options.quiet_) {
			std::cout << "\"" << options.command_->name() << "\" command successful." <<  std::endl;

		}
	}
	catch (ToolException& e) {
		std::cerr << e.what() << "." << std::endl;
		returnCode = e.code();
	}
	catch (kerio::hashdb::DataError& e) {
		std::cerr << e.what() << std::endl;
		returnCode = OutputDatabaseCorruptedReturnCode;
	}
	catch (kerio::hashdb::IoException& e) {
		std::cerr << e.what() << std::endl;
		returnCode = OutputDatabaseIoErrorReturnCode;
	}
	catch (kerio::hashdb::Exception& e) {
		std::cerr << e.what() << std::endl;
		returnCode = GenericErrorReturnCode;
	}
	catch (std::bad_alloc&) {
		std::cerr << "Out of memory" << std::endl;
		returnCode = OutOfMemoryReturnCode;
	}
	catch (std::exception& e) {
		std::cerr << "HashDB command line tool terminated by exception: " << e.what() << std::endl;
		returnCode = GenericErrorReturnCode;
	}

	return returnCode;
}

int main(int argc, char** argv) 
{
	ReturnCode returnCode = SuccessReturnCode;

	if (argc < 2) {
		std::cout << CommandOptions::usage() << std::endl;
	}
	else {
		returnCode = run(argc, argv);
	}

	return returnCode;
}
