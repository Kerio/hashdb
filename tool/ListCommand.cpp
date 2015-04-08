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
#include <kerio/hashdb/HashDB.h>
#include <kerio/hashdb/Exception.h>
#include "utils/ExceptionCreator.h"
#include "CommandOptions.h"
#include "ListCommand.h"

namespace kerio {
namespace hashdb {
namespace tool {

	class ListCommand : public ICommand {
	public:
		virtual std::string name()
		{
			return "list";
		}

		virtual bool isReadOnly()
		{
			return true;
		}

		virtual void run(const CommandOptions& options)
		{
			Database db = options.openDatabaseReadOnly();

			Iterator databaseIterator = db->newIterator();

			std::cout << "[" << std::endl << "  ";

			while (databaseIterator->isValid()) {
				std::cout << "{" << std::endl << "    ";
				printJsonNameValue(std::cout, "key", databaseIterator->key());

				std::cout << "," << std::endl << "    ";
				printJsonNameValue(std::cout, "partNum", databaseIterator->partNum());

				std::cout << "," << std::endl << "    ";
				printJsonNameValue(std::cout, "value", databaseIterator->value());
				std::cout << std::endl << "  }";

				databaseIterator->next();
				if (databaseIterator->isValid()) {
					std::cout << ", ";
				}
			}

			std::cout << std::endl << "]" << std::endl;
			db->close();
		}
	};


	Command newListCommand()
	{
		Command newInstance(new ListCommand());
		return newInstance;
	}

}; // namespace tool
}; // namespace hashdb
}; // namespace kerio
