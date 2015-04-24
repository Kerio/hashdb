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
#include "utils/ExceptionCreator.h"
#include "utils/CommandLine.h"
#include "HashdbWrapper.h"
#include "BdbWrapper.h"
#include "NullDatabaseWrapper.h"
#include "BenchmarkOptions.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	std::string BenchmarkOptions::usage()
	{
		std::ostringstream os;

		os << "Usage: {write|read|update|delete} [options...]" << std::endl;
		os << std::endl;
		os << "Benchmark options:" << std::endl;
		os << "  --threads=N  ................. number of threads (default is 1)" << std::endl;
		os << "  --num=N ...................... number of iterations" << std::endl;
		os << "  --start=N .................... start of the sequence" << std::endl;
		os << "  --random ..................... random keys" << std::endl;
		os << "  --max=N ...................... maximum random key" << std::endl;
		os << "  --key=<generator> ............ key type (default is int_be)" << std::endl;
		os << "  --value=<generator> .......... value type (default is props)" << std::endl;
		os << "  --keyPrefix=N --valuePrefix=N  prepend N constant bytes to each key or value" << std::endl;
		os << "  --verbose .................... verbose output" << std::endl;
		os << "  --disable-csv ................ disable output to \"results.csv\"" << std::endl;
		os << "  --label=str .................. label for CSV output" << std::endl;
		os << "Database options:" << std::endl;
		os << "  --use={hashdb|btree|hash|null} use database type (default is hashdb)" << std::endl;
		os << "  --db=name .................... database name" << std::endl;
		os << "  --new ........................ create new database" << std::endl;
		os << "  --pageSize=N ................. page size (default is 4 KB)" << std::endl;
		os << "  --cache=N .................... cache size in bytes (default is 10 KB)" << std::endl;
		os << "  --initialBuckets=N ........... hashdb or hash: initial number of buckets" << std::endl;
		os << "  --hashdbPageFreeSpace=N ...... hashdb: free space to be left page" << std::endl;
		os << "  --hashdbFlushFrequency=N ..... hashdb: number of operations before cache is flushed" << std::endl;
		os << "  --hashdbPrefetch ............. hashdb: prefetch database to OS caches" << std::endl;
		os << "  --hashdbBatchSize=N .......... hashdb: batch size" << std::endl;
		os << "  --hashdbLog .................. output hashdb operations" << std::endl;
		os << "  --hash-ffactor=N ............. hash: fill factor (default is automatic)" << std::endl;
		os << "Key or value generator is one of: null, int or int_be (4 bytes big endian), int_le (4 bytes little endian), str (English representation), props" << std::endl;
		return os.str();
	}

	BenchmarkOptions::BenchmarkOptions(int argc, char** argv)
		: numberOfThreads_(1)
		, verbose_(false)
		, hashdbLog_(false)
		, disableResultsCsv_(false)
		, createNew_(false)
		, pageSize_(2048)
		, cacheSize_(10240)
		, initialBuckets_(SIZE_VALUE_NOT_SET)
		, hashdbPrefetch_(false)
		, hashdbPageFreeSpace_(0)
		, hashdbFlushFrequency_(SIZE_VALUE_NOT_SET)
		, hashdbBatchSize_(SIZE_VALUE_NOT_SET)
		, bdbHashFfactor_(SIZE_VALUE_NOT_SET)
	{
		CommandLine arguments(argc, argv);

		RAISE_INVALID_ARGUMENT_IF(arguments.count() < 1, "not enough arguments");

		// Database type.
		const boost::string_ref dbType = arguments.optionRef("--use");
		if (dbType.empty() || dbType == "hashdb") {
			databaseWrapperFactory_ = newHashdbWapperFactory();
		}
		else if (dbType == "btree") {
			databaseWrapperFactory_ = newBdbBtreeWrapperFactory();
		}
		else if (dbType == "hash") {
			databaseWrapperFactory_ = newBdbHashWrapperFactory();
		}
		else if (dbType == "null") {
			databaseWrapperFactory_ = newNullDatabaseWrapperFactory();
		}
		else {
			RAISE_INVALID_ARGUMENT("Unknown database type \"%s\"", dbType.to_string());
		}

		// Benchmark type.
		if (arguments.hasOption("read")) {
			if (databaseWrapperFactory_->name() == "hashdb" && arguments.optionUnsignedNumericValue(hashdbBatchSize_, "--hashdbBatchSize")) {
				RAISE_INVALID_ARGUMENT_IF(hashdbBatchSize_ == 0, " --hashdbBatchSize must not be 0");
				benchmarkFactory_ = newBatchReadBenchmarkFactory();
			}
			else {
				benchmarkFactory_ = newReadBenchmarkFactory();
			}
		}
		else if (arguments.hasOption("write")) {
			if (databaseWrapperFactory_->name() == "hashdb" && arguments.optionUnsignedNumericValue(hashdbBatchSize_, "--hashdbBatchSize")) {
				RAISE_INVALID_ARGUMENT_IF(hashdbBatchSize_ == 0, " --hashdbBatchSize must not be 0");
				benchmarkFactory_ = newBatchWriteBenchmarkFactory();
			}
			else {
				benchmarkFactory_ = newWriteBenchmarkFactory();
			}
		}
		else if (arguments.hasOption("update")) {
			benchmarkFactory_ = newUpdateBenchmarkFactory();
		}
		else if (arguments.hasOption("delete")) {
			benchmarkFactory_ = newDeleteBenchmarkFactory();
		}
		else {
			RAISE_INVALID_ARGUMENT("benchmark type not specified");
		}

		// Number of threads.
		arguments.optionUnsignedNumericValue(numberOfThreads_, "--threads");

		// Sequence.
		size_type iterations = 0;
		if (arguments.optionUnsignedNumericValue(iterations, "--num")) {
			RAISE_INVALID_ARGUMENT_IF(iterations == 0, "number of iterations cannot be 0");

			if (arguments.hasOption("--random")) {
				size_type maxValue = iterations - 1;
				arguments.optionUnsignedNumericValue(maxValue, "--max");
				sequenceFactory_ = newRandomSequenceFactory(maxValue, iterations);
			}
			else {
				size_type start = 0;
				arguments.optionUnsignedNumericValue(start, "--start");

				sequenceFactory_ = newSimpleSequenceFactory(start, iterations);
			}

		}
		else {
			sequenceFactory_ = benchmarkFactory_->defaultSequenceFactory();
		}

		// Key and value types.
		size_type keyPrefixSize = 0;
		std::string keyPrefix;
		if (arguments.optionUnsignedNumericValue(keyPrefixSize, "--keyPrefix")) {
			keyPrefix.resize(keyPrefixSize, '0');
		}

		const boost::string_ref keyType = arguments.optionRef("--key");
		if (keyType.empty() || keyType == "int_be" || keyType == "int") {
			keyProducer_ = newInt32BigEndianDataProducer(keyPrefix);
		}
		else if (keyType == "int_le") {
			keyProducer_ = newInt32LittleEndianDataProducer(keyPrefix);
		}
		else if (keyType == "str") {
			keyProducer_ = newStringDataProducer(keyPrefix);
		}
		else {
			RAISE_INVALID_ARGUMENT("Unknown key type \"%s\"", keyType.to_string());
		}

		size_type valuePrefixSize = 0;
		std::string valuePrefix;
		if (arguments.optionUnsignedNumericValue(valuePrefixSize, "--valuePrefix")) {
			valuePrefix.resize(valuePrefixSize, '0');
		}

		const boost::string_ref valueType = arguments.optionRef("--value");
		if (valueType.empty() || valueType == "props") {
			valueProducer_ = newPropsDataProducer(valuePrefix);
		}
		else if (keyType == "int_be" || keyType == "int") {
			valueProducer_ = newInt32BigEndianDataProducer(valuePrefix);
		}
		else if (valueType == "int_le") {
			valueProducer_ = newInt32LittleEndianDataProducer(valuePrefix);
		}
		else if (valueType == "str") {
			valueProducer_ = newStringDataProducer(valuePrefix);
		}
		else if (valueType == "null") {
			valueProducer_ = newNullDataProducer(valuePrefix);
		}
		else {
			RAISE_INVALID_ARGUMENT("Unknown value type \"%s\"", valueType.to_string());
		}

		// Output.
		verbose_ = arguments.hasOption("--verbose");
		disableResultsCsv_ = arguments.hasOption("--disable-csv");
		csvLabel_ = arguments.optionRef("--label").to_string();

		// Database name.
		const boost::string_ref db = arguments.optionRef("--db");
		if (! db.empty()) {
			databaseName_ = db.to_string();
		}
		else {
			databaseName_ = "db_" + databaseWrapperFactory_->name();
		}

		// Database options.
		createNew_ = arguments.hasOption("--new");

		arguments.optionUnsignedNumericValue(pageSize_, "--pageSize");
		arguments.optionUnsignedNumericValue(cacheSize_, "--cache");
		arguments.optionUnsignedNumericValue(initialBuckets_, "--initialBuckets");

		if (databaseWrapperFactory_->name() == "hashdb") {
			hashdbLog_ = arguments.hasOption("--hashdbLog");
			hashdbPrefetch_ = arguments.hasOption("--hashdbPrefetch");
		
			arguments.optionSignedNumericValue(hashdbPageFreeSpace_, "--hashdbPageFreeSpace");
			arguments.optionUnsignedNumericValue(hashdbFlushFrequency_, "--hashdbFlushFrequency");
		}

		if (databaseWrapperFactory_->name() == "hash") {
			arguments.optionUnsignedNumericValue(bdbHashFfactor_, "--hash-ffactor");
		}

		// All options should be processed now.
		const std::string unusedOptions = arguments.listUnusedOptions();
		RAISE_INVALID_ARGUMENT_IF(! unusedOptions.empty(), "unrecognized option(s): %s", unusedOptions);
	}

	std::string BenchmarkOptions::list() const
	{
		std::ostringstream os;

		// Benchmark settings.
		os << "* Running " << benchmarkFactory_->name() << " benchmark for " << databaseWrapperFactory_->name() << " database (";
		if (numberOfThreads_ != 1) {
			os << numberOfThreads_ << "threads, ";
		}

		os << sequenceFactory_->size() << " iterations, ";
		os << "key=" << keyProducer_->name() << ", ";
		os << "value=" << valueProducer_->name() << ", ";
		os << sequenceFactory_->name() << ")" << std::endl;


		// Database settings.
		os << "* Database: name=\"" << databaseName_ << "\", create new: " << ((createNew_)? "true" : "false");
		
		if (hashdbLog_) {
			os << ", logging";
		}

		listValueOrNothing(os, "pageSize", pageSize_,  "bytes");				// HashdbWapper, BdbHashWrapper, BdbBtreeWrapper
		listValueOrNothing(os, "cache", cacheSize_,  "bytes");					// HashdbWapper, BdbHashWrapper, BdbBtreeWrapper
		listValueOrNothing(os, "initial buckets", initialBuckets_);				// HashdbWapper, BdbHashWrapper
		listValueOrNothing(os, "hashdbPageFreeSpace", hashdbPageFreeSpace_,  "bytes"); // HashdbWapper
		listValueOrNothing(os, "hashdbFlushFrequency", hashdbFlushFrequency_);	// HashdbWapper
		listValueOrNothing(os, "batchSize", hashdbBatchSize_);					// HashdbWapper

		if (hashdbPrefetch_) {													// HashdbWapper
			os << ", hashdbPrefetch";
		}

		listValueOrNothing(os, "hash-ffactor", bdbHashFfactor_, "records");		// BdbHashWrapper

		return os.str();
	}

	void BenchmarkOptions::listValueOrNothing(std::ostringstream& os, const char* name, size_type value,  const char* unit) const
	{
		if (value != SIZE_VALUE_NOT_SET) {
			os << ", " << name << " = " << value << " " << unit;
		}
	}

	void BenchmarkOptions::listValueOrNothing(std::ostringstream& os, const char* name, int32_t value,  const char* unit) const
	{
		if (value != 0) {
			os << ", " << name << " = " << value << " " << unit;
		}
	}

	std::string BenchmarkOptions::csv(double seconds) const
	{
		std::ostringstream os;

		if (! csvLabel_.empty()) {
			os << csvLabel_ << ",";
		}

		const size_type sequenceSize = sequenceFactory_->size();
		double opsPerSecond = 0;
		if (seconds != 0) {
			opsPerSecond = (sequenceSize * numberOfThreads_) / seconds;
		}
		os	<< "\"" << benchmarkFactory_->name() << "\"," 
					<< numberOfThreads_ << ",\"" 
					<< keyProducer_->name() << "\",\"" 
					<< valueProducer_->name() << "\"," 
					<< sequenceFactory_->size() << "," 
					<< seconds << "," 
					<< opsPerSecond;
		return os.str();
	}

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
