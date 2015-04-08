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
#include <sstream>
#include <kerio/hashdb/Exception.h>
#include <kerio/hashdbHelpers/StdoutLogger.h>
#include "utils/CommandLine.h"
#include "Exception.h"
#include "ListCommand.h"
#include "StatsCommand.h"
#include "CommandOptions.h"

namespace kerio {
namespace hashdb {
namespace tool {

	std::string CommandOptions::usage()
	{
		std::ostringstream os;

		os << "Usage: command [options...]" << std::endl;
		os << "Commands:" << std::endl;
		os << "  list ......................... list entire database as JSON" << std::endl;
		os << "  stats ........................ print database statistics" << std::endl;
		os << std::endl;
		os << "Options:" << std::endl;
		os << "  --db=name .................... database name (default is \"metadata\")" << std::endl;
		os << "  --dir=directory .............. directory name" << std::endl;
		os << "  --log ........................ enable debug logging to stdout" << std::endl;
		os << "  --quiet ...................... quiet execution of the command" << std::endl;
		return os.str();
	}

	CommandOptions::CommandOptions(int argc, char** argv)
	{
		CommandLine arguments(argc, argv);

		if (arguments.count() < 1) {
			RAISE_TOOL_EXCEPTION(BadCommandOptionsReturnCode, "not enough arguments");
		}


		// Operation type.
		if (arguments.hasOption("list")) {
			command_ = newListCommand();
		}
		else if (arguments.hasOption("stats")) {
			command_ = newStatsCommand();
		}
		else {
			RAISE_TOOL_EXCEPTION(BadCommandOptionsReturnCode, "operation type not specified");
		}

		// Directory.
		const boost::string_ref dirOption = arguments.optionRef("--dir");
		if (! dirOption.empty()) {
			dir_ = dirOption.to_string();
			if (! dirOption.ends_with("/") && ! dirOption.ends_with("\\")) {
				dir_ += "/";
			}
		}

		// Database name.
		const boost::string_ref dbOption = arguments.optionRef("--db");
		if (! dbOption.empty()) {
			databaseName_ = dir_ + dbOption.to_string();
		}
		else {
			databaseName_ += dir_ + "metadata";
		}

		log_ = arguments.hasOption("--log");
		quiet_ = arguments.hasOption("--quiet");
	}

	Database CommandOptions::openDatabaseReadOnly() const
	{
		Database db = DatabaseFactory();
		Options options = Options::readOnlySingleThreaded();

		if (log_) {
			options.logger_.reset(new StdoutLogger);
		}

		db->open(databaseName_, options);
		return db;
	}

	Database CommandOptions::createNewDatabase(size_t preallocatedSize /* = 0 */) const
	{
		Database db = DatabaseFactory();

		if (db->exists(databaseName_)) {
			RAISE_TOOL_EXCEPTION(OutputDatabaseExistsReturnCode, "output database \"%s\" already exists", databaseName_);
		}

		Options options = Options::readWriteSingleThreaded();
		options.initialBuckets_ = static_cast<kerio::hashdb::size_type>(preallocatedSize / options.pageSize_  + 1);

		if (log_) {
			options.logger_.reset(new StdoutLogger);
		}

		db->open(databaseName_, options);
		return db;
	}

	std::string CommandOptions::list() const
	{
		std::ostringstream os;

		// Tool settings.
		os << "// Performing '" << command_->name() << "' command, " << (command_->isReadOnly()? "input" : "output") << " database is \"" << databaseName_ << "\"";
		if (log_) {
			os << ", logging to stdout";
		}

		return os.str();
	}

}; // namespace tool
}; // namespace hashdb
}; // namespace kerio
