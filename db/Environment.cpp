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

// Environment.cpp - environment for accessing the database.
#include "stdafx.h"
#include "NullLockManager.h"
#include "SimplePageAllocator.h"
#include "SingleThreadedPageAllocator.h"
#include "utils/ExceptionCreator.h"
#include "Environment.h"

namespace kerio {
namespace hashdb {

	Environment::Environment(const Options& options)
	{
		options.validate();

		switch (options.lockManagerType_) {
		case Options::NullLockManagerType:
			lockManager_.reset(new NullLockManager());
			break;

		default:
			RAISE_NOT_YET_IMPLEMENTED("Option lockManagerType_=%d is not yet implemented", options.lockManagerType_);
			break;
		}

		switch (options.pageAllocatorType_) {
		case Options::SimplePageAllocatorType:
			pageAllocator_.reset(new SimplePageAllocator());
			break;

		case Options::SingleThreadedPageAllocatorType:
			pageAllocator_.reset(new SingleThreadedPageAllocator(options.memoryPoolBytes_));
			headerPageAllocator_.reset(new SimplePageAllocator());
			break;

		default:
			RAISE_NOT_YET_IMPLEMENTED("Option pageAllocatorType_=%d is not yet implemented", options.pageAllocatorType_);
			break;
		}

		logger_ = options.logger_;
	}

	//----------------------------------------------------------------------------
	// Accessors.

	IPageAllocator* Environment::pageAllocator()
	{
		return pageAllocator_.get();
	}

	IPageAllocator* Environment::headerPageAllocator()
	{
		return (headerPageAllocator_)? headerPageAllocator_.get() : pageAllocator_.get();
	}

	ILockManager* Environment::lockManager()
	{
		return lockManager_.get();
	}

	//----------------------------------------------------------------------------
	// Logging.

	bool Environment::isDebugLogEnabled()
	{
		return logger_ && logger_->isDebugLogEnabled();
	}

	void Environment::logDebug(const std::string& logMessage)
	{
		logger_->logDebug(logMessage);
	}

}; // namespace hashdb
}; // namespace kerio
