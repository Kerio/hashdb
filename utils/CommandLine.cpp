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
#include <stdio.h>
#include <kerio/hashdb/Exception.h>
#include "ExceptionCreator.h"
#include "CommandLine.h"

namespace kerio {
namespace hashdb {

	CommandLine::CommandLine(int argc, char** argv)
		: argc_(argc)
		, argv_(argv)
	{
		usedOptions_.resize(argc);
		usedOptions_[0] = true;
	}

	boost::string_ref CommandLine::findOption(boost::string_ref optionName)
	{
		boost::string_ref rv;

		for (int i = 1; i < argc_; ++i) {
			boost::string_ref thisOption(argv_[i]);
			if (thisOption == optionName || thisOption.starts_with(optionName.to_string() + "=")) {
				rv = thisOption;
				usedOptions_[i] = true;
				break;
			}
		}

		return rv;
	}

	bool CommandLine::hasOption(boost::string_ref optionName)
	{
		boost::string_ref found = findOption(optionName);
		return ! found.empty();
	}

	boost::string_ref CommandLine::optionRef(boost::string_ref optionName)
	{
		boost::string_ref rv;
		boost::string_ref found = findOption(optionName);

		if (found.size() > optionName.size() + 1) {
			found.remove_prefix(optionName.size());

			if (found.front() == '=') {
				found.remove_prefix(1);
				rv = found;
			}
		}

		return rv;
	}

	bool CommandLine::optionUnsignedNumericValue(size_type& value, boost::string_ref optionName)
	{
		boost::string_ref refValue = optionRef(optionName);
		bool success = false;

		if (!refValue.empty()) {
			const std::string scanValue = refValue.to_string();
			unsigned scanResult;

			if (sscanf(scanValue.c_str(), "%u", &scanResult) == 1) {
				value = scanResult;
				success = true;
			}
		}

		return success;
	}


	bool CommandLine::optionSignedNumericValue(int32_t& value, boost::string_ref optionName)
	{
		boost::string_ref refValue = optionRef(optionName);
		bool success = false;

		if (!refValue.empty()) {
			const std::string scanValue = refValue.to_string();
			int scanResult;

			if (sscanf(scanValue.c_str(), "%d", &scanResult) == 1) {
				value = scanResult;
				success = true;
			}
		}

		return success;
	}

	kerio::hashdb::size_type CommandLine::count()
	{
		return static_cast<size_type>(argc_ - 1);
	}

	void CommandLine::raiseIfUnused()
	{
		std::string unusedOptions;

		for (int i = 1; i < argc_; ++i) {
			if (! usedOptions_[i]) {
				if (! unusedOptions.empty()) {
					unusedOptions += ", ";
				}

				unusedOptions += argv_[i];
			}
		}
		
		RAISE_INVALID_ARGUMENT_IF(! unusedOptions.empty(), "unrecognized option(s): %s", unusedOptions);
	}

}; // namespace hashdb
}; // namespace kerio
