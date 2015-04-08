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

// BatchApi.h -- interface to be implemented by user-defined batch classes for batch hashdb fetch, store or remove operations.
//
// For example, you can fetch an attribute for several messages in a single batch. For this you need to implement the IReadBatch
// interface and pass an instance of your read batch class to the db->fetch(readBatch) method. It is up to the read batch class 
// to decide what to do with the retrieved values - some batch classes can e.g. process the retrieved values without storing 
// them at all, other implementations may store them to existing buffers etc.

#pragma once
#include <boost/utility/string_ref.hpp>
#include <istream>
#include <kerio/hashdb/Types.h>

namespace kerio {
namespace hashdb {

	class StringOrReference;

	class IKeyProducer {
	public:
		// Returns the number of key/part number pairs which can be retrieved from the class.
		virtual size_t count() const = 0;

		// Returns key at the given index.
		virtual StringOrReference keyAt(size_t index) const = 0;

		// Returns the part number at the given index.
		virtual partNum_t partNumAt(size_t index) const = 0;

		virtual ~IKeyProducer() { }
	};

	class IValueProducer {
	public:
		// Returns a reference to the value at the given index.
		virtual StringOrReference valueAt(size_t index) const = 0;

		virtual ~IValueProducer() { }
	};

	class IValueConsumer {
	public:
		// Sets the value at the given index. Returns true if the value is successfully
		// set, returns false otherwise (e.g. if the value does not fit into a buffer).
		// This method is called for database records that are stored on a single database page.
		virtual bool setValueAt(size_t index, const boost::string_ref& value) = 0;

		// Sets the large value at the given index. Returns true if the value is successfully
		// set, returns false otherwise (e.g. if the value does not fit into a buffer).
		// This method is called for database records stored on multiple database pages.
		virtual bool setLargeValueAt(size_t index, std::istream& valueStream, size_t valueSize) = 0;

		virtual ~IValueConsumer() { }
	};

	//------------------------------------------------------------------------
	// Interface for the ReadBatch. It lets hashdb to read individual keys/part numbers 
	// from the class and to write back individual values retrieved from the database.
	// Example implementation: class StringReadBatch in the "kerio/hashdbHelpers" directory.
	class IReadBatch 
		: public IKeyProducer
		, public IValueConsumer
	{

	};

	// Interface for the WriteBatch. It lets hashdb to read individual keys, part numbers
	// and values from the class and write them to the database.
	// Example implementation: class StringWriteBatch in the "kerio/hashdbHelpers" directory.
	class IWriteBatch
		: public IKeyProducer
		, public IValueProducer
	{

	};
		
	// Interface for the DeleteBatch. It lets hashdb to read individual keys and part numbers
	// to be removed from the database.
	// Example implementation: class DeleteBatch in the "kerio/hashdbHelpers" directory.
	class IDeleteBatch
		: public IKeyProducer
	{

	};


}; // namespace hashdb
}; // namespace kerio
