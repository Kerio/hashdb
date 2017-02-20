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
#include <kerio/hashdb/Exception.h>
#include <kerio/hashdb/BatchApi.h>
#include <kerio/hashdbHelpers/StringWriteBatch.h>
#include <kerio/hashdbHelpers/StringReadBatch.h>
#include "utils/ExceptionCreator.h"
#include "utils/StopWatch.h"
#include "BenchmarkException.h"
#include "HashdbWrapper.h"
#include "ProgressReporter.h"
#include "BenchmarkOptions.h"
#include "Benchmark.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	//-------------------------------------------------------------------------
	// Write benchmark.

	class WriteBenchmark : public IBenchmark
	{
	public:
		WriteBenchmark(size_type instanceNumber)
			: instanceNumber_(instanceNumber)
		{

		}

		virtual void prepare(const BenchmarkOptions& options)
		{
			iterations_ = options.sequenceFactory_->size();
			sequence_ = options.sequenceFactory_->newSequence();
			database_ = options.databaseWrapperFactory_->open(options, instanceNumber_);
			keyProducer_ = options.keyProducer_;
			valueProducer_ = options.valueProducer_;
		}

		virtual void run()
		{
			ProgressReporter reporter(iterations_);

			std::string key;
			std::string value;

			while (sequence_->hasNext()) {
				const size_type number = sequence_->next();

				keyProducer_->dataFor(key, number);
				valueProducer_->dataFor(value, number);

				database_->store(key, value);

				reporter.next();
			}

			database_->close();
			reporter.finish();
		}

		virtual size_type instance()
		{
			return instanceNumber_;
		}

	private:
		size_type instanceNumber_;
		size_type iterations_;
		Sequence sequence_;
		DatabaseWrapper database_;
		DataProducer keyProducer_;
		DataProducer valueProducer_;
	};

	class WriteBenchmarkFactory : public IBenchmarkFactory {
	public:
		virtual Benchmark newBenchmark(size_type instanceNumber /* = 0 */)
		{
			Benchmark newInstance(new WriteBenchmark(instanceNumber));
			return newInstance;
		}

		virtual std::string name() const
		{
			return "write";
		}

		virtual bool isBatch()
		{
			return false;
		}

		virtual SequenceFactory defaultSequenceFactory()
		{
			return newSimpleSequenceFactory(0, DEFAULT_WRITE_ITERATIONS);
		}
	};

	BenchmarkFactory newWriteBenchmarkFactory()
	{
		BenchmarkFactory newInstance(new WriteBenchmarkFactory());
		return newInstance;
	}

	//-------------------------------------------------------------------------
	// Update benchmark.

	class UpdateBenchmark : public IBenchmark
	{
	public:
		UpdateBenchmark(size_type instanceNumber)
			: instanceNumber_(instanceNumber)
		{

		}

		virtual void prepare(const BenchmarkOptions& options)
		{
			iterations_ = options.sequenceFactory_->size();
			keySequence_ = options.sequenceFactory_->newSequence();
			valueSequence_ = options.sequenceFactory_->newSequence();
			database_ = options.databaseWrapperFactory_->open(options, instanceNumber_);
			keyProducer_ = options.keyProducer_;
			valueProducer_ = options.valueProducer_;
		}

		virtual void run()
		{
			ProgressReporter reporter(iterations_);

			std::string key;
			std::string value;

			while (keySequence_->hasNext()) {
				const size_type keyNumber = keySequence_->next();
				keyProducer_->dataFor(key, keyNumber);

				const size_type valueNumber = valueSequence_->next();
				valueProducer_->dataFor(value, valueNumber);

				database_->store(key, value);

				reporter.next();
			}

			database_->close();
			reporter.finish();
		}

		virtual size_type instance()
		{
			return instanceNumber_;
		}

	private:
		size_type instanceNumber_;
		size_type iterations_;
		Sequence keySequence_;
		Sequence valueSequence_;
		DatabaseWrapper database_;
		DataProducer keyProducer_;
		DataProducer valueProducer_;
	};

	class UpdateBenchmarkFactory : public IBenchmarkFactory {
	public:
		virtual Benchmark newBenchmark(size_type instanceNumber /* = 0 */)
		{
			Benchmark newInstance(new UpdateBenchmark(instanceNumber));
			return newInstance;
		}

		virtual std::string name() const
		{
			return "update";
		}

		virtual bool isBatch()
		{
			return false;
		}

		virtual SequenceFactory defaultSequenceFactory()
		{
			return newRandomSequenceFactory(DEFAULT_WRITE_ITERATIONS - 1, DEFAULT_UPDATE_ITERATIONS);
		}
	};

	BenchmarkFactory newUpdateBenchmarkFactory()
	{
		BenchmarkFactory newInstance(new UpdateBenchmarkFactory());
		return newInstance;
	}

	//-------------------------------------------------------------------------
	// Read benchmark.

	class ReadBenchmark : public IBenchmark
	{
	public:
		ReadBenchmark(size_type instanceNumber)
			: instanceNumber_(instanceNumber)
		{

		}

		virtual void prepare(const BenchmarkOptions& options)
		{
			iterations_ = options.sequenceFactory_->size();
			sequence_ = options.sequenceFactory_->newSequence();
			database_ = options.databaseWrapperFactory_->open(options, instanceNumber_);
			keyProducer_ = options.keyProducer_;
			valueProducer_ = options.valueProducer_;
		}

		virtual void run()
		{
			ProgressReporter reporter(iterations_);

			std::string key;
			std::string expectedValue;
			std::string actualValue;

			while (sequence_->hasNext()) {
				const size_type number = sequence_->next();

				keyProducer_->dataFor(key, number);
				valueProducer_->dataFor(expectedValue, number);

				database_->checkedFetch(number, key, expectedValue);

				reporter.next();
			}

			database_->close();
			reporter.finish();
		}

		virtual size_type instance()
		{
			return instanceNumber_;
		}

	private:
		size_type instanceNumber_;
		size_type iterations_;
		Sequence sequence_;
		DatabaseWrapper database_;
		DataProducer keyProducer_;
		DataProducer valueProducer_;
	};

	class ReadBenchmarkFactory : public IBenchmarkFactory {
	public:
		virtual Benchmark newBenchmark(size_type instanceNumber /* = 0 */)
		{
			Benchmark newInstance(new ReadBenchmark(instanceNumber));
			return newInstance;
		}

		virtual std::string name() const
		{
			return "read";
		}

		virtual bool isBatch()
		{
			return false;
		}

		virtual SequenceFactory defaultSequenceFactory()
		{
			return newSimpleSequenceFactory(0, DEFAULT_READ_ITERATIONS);
		}
	};

	BenchmarkFactory newReadBenchmarkFactory()
	{
		BenchmarkFactory newInstance(new ReadBenchmarkFactory());
		return newInstance;
	}

	//-------------------------------------------------------------------------
	// Delete benchmark.

	class DeleteBenchmark : public IBenchmark
	{
	public:
		DeleteBenchmark(size_type instanceNumber)
			: instanceNumber_(instanceNumber)
		{

		}

		virtual void prepare(const BenchmarkOptions& options)
		{
			iterations_ = options.sequenceFactory_->size();
			sequence_ = options.sequenceFactory_->newSequence();
			database_ = options.databaseWrapperFactory_->open(options, instanceNumber_);
			keyProducer_ = options.keyProducer_;
		}

		virtual void run()
		{
			ProgressReporter reporter(iterations_);

			std::string key;

			while (sequence_->hasNext()) {
				const size_type number = sequence_->next();
				keyProducer_->dataFor(key, number);
				database_->remove(key);

				reporter.next();
			}

			database_->close();
			reporter.finish();
		}

		virtual size_type instance()
		{
			return instanceNumber_;
		}

	private:
		size_type instanceNumber_;
		size_type iterations_;
		Sequence sequence_;
		DatabaseWrapper database_;
		DataProducer keyProducer_;
	};

	class DeleteBenchmarkFactory : public IBenchmarkFactory {
	public:
		virtual Benchmark newBenchmark(size_type instanceNumber /* = 0 */)
		{
			Benchmark newInstance(new DeleteBenchmark(instanceNumber));
			return newInstance;
		}

		virtual std::string name() const
		{
			return "delete";
		}

		virtual bool isBatch()
		{
			return false;
		}

		virtual SequenceFactory defaultSequenceFactory()
		{
			return newRandomSequenceFactory(DEFAULT_WRITE_ITERATIONS - 1, DEFAULT_DELETE_ITERATIONS);
		}
	};

	BenchmarkFactory newDeleteBenchmarkFactory()
	{
		BenchmarkFactory newInstance(new DeleteBenchmarkFactory());
		return newInstance;
	}

	//-------------------------------------------------------------------------
	// Batch write benchmark.

	class BatchWriteBenchmark : public IBenchmark
	{
	public:
		BatchWriteBenchmark(size_type instanceNumber)
			: instanceNumber_(instanceNumber)
		{

		}

		virtual void prepare(const BenchmarkOptions& options)
		{
			iterations_ = options.sequenceFactory_->size();
			batchSize_ = options.hashdbBatchSize_;
			sequence_ = options.sequenceFactory_->newSequence();
			keyProducer_ = options.keyProducer_;
			valueProducer_ = options.valueProducer_;

			DatabaseWrapper databaseWrapper = options.databaseWrapperFactory_->open(options, instanceNumber_);
			db_ = (dynamic_cast<HashdbWapper*>(databaseWrapper.get()))->database();
		}

		virtual void run()
		{
			ProgressReporter reporter(iterations_);

			std::string key;
			std::string value;

			StringWriteBatch writeBatch;
			while (sequence_->hasNext()) {
				const size_type number = sequence_->next();

				keyProducer_->dataFor(key, number);
				valueProducer_->dataFor(value, number);

				writeBatch.add(key, 0, value);

				if (writeBatch.count() >= batchSize_ || ! sequence_->hasNext()) {
					db_->store(writeBatch);
					writeBatch.clear();
				}

				reporter.next();
			}

			db_->close();
			reporter.finish();
		}

		virtual size_type instance()
		{
			return instanceNumber_;
		}

	private:
		size_type instanceNumber_;
		size_type iterations_;
		size_type batchSize_;
		Sequence sequence_;
		Database db_;
		DataProducer keyProducer_;
		DataProducer valueProducer_;
	};

	class BatchWriteBenchmarkFactory : public IBenchmarkFactory {
	public:
		virtual Benchmark newBenchmark(size_type instanceNumber /* = 0 */)
		{
			Benchmark newInstance(new BatchWriteBenchmark(instanceNumber));
			return newInstance;
		}

		virtual std::string name() const
		{
			return "batchWrite";
		}

		virtual bool isBatch()
		{
			return true;
		}

		virtual SequenceFactory defaultSequenceFactory()
		{
			return newSimpleSequenceFactory(0, DEFAULT_WRITE_ITERATIONS);
		}
	};

	BenchmarkFactory newBatchWriteBenchmarkFactory()
	{
		BenchmarkFactory newInstance(new BatchWriteBenchmarkFactory());
		return newInstance;
	}

	//-------------------------------------------------------------------------
	// Batch read benchmark.

	class BenchmarkReadBatch : public IReadBatch { // intentionally copyable
	public:
		void reserve(size_type n)
		{
			records_.reserve(n);
		}

		void add(size_type number, const std::string& key)
		{
			Record newRecord(number, key);
			records_.push_back(newRecord);
		}

		std::string resultAt(size_type index)
		{
			return records_.at(index).value_;
		}

		size_type numberAt(size_type index)
		{
			return records_.at(index).number_;
		}

		void clear()
		{
			records_.clear();
		}

		virtual size_t count() const
		{
			return records_.size();
		}

		virtual StringOrReference keyAt(size_t index) const
		{
			return StringOrReference::reference(records_.at(index).key_);
		}

		virtual partNum_t partNumAt(size_t index) const
		{
			return 0;
		}

		virtual bool setValueAt(size_t index, const boost::string_ref& value)
		{
			records_.at(index).value_.assign(value.begin(), value.end());
			return true;
		}

		virtual bool setLargeValueAt(size_t index, std::istream& valueStream, size_t valueSize)
		{
			records_.at(index).value_.resize(valueSize);
			valueStream.read(&records_.at(index).value_[0], valueSize);
			return static_cast<size_t>(valueStream.gcount()) == valueSize;
		}

	private:
		struct Record {
			Record(size_type number, const std::string& key)
				: number_(number)
				, key_(key)
			{ }

			size_type number_;
			std::string key_;
			std::string value_;
		};

		std::vector<Record> records_;
	};

	class BatchReadBenchmark : public IBenchmark
	{
	public:
		BatchReadBenchmark(size_type instanceNumber)
			: instanceNumber_(instanceNumber)
		{

		}

		void validateReadBatch(BenchmarkReadBatch& readBatch)
		{
			const size_type count = static_cast<size_type>(readBatch.count());
			std::string expectedValue;

			for (size_type i = 0; i < count; ++i) {
				valueProducer_->dataFor(expectedValue, readBatch.numberAt(i));
				if (expectedValue != readBatch.resultAt(i)) {
					RAISE_BENCHMARK_EXCEPTION("unexpected value for key number %u", readBatch.numberAt(i));
				}
			}
		}

		virtual void prepare(const BenchmarkOptions& options)
		{
			iterations_ = options.sequenceFactory_->size();
			batchSize_ = options.hashdbBatchSize_;
			sequence_ = options.sequenceFactory_->newSequence();
			keyProducer_ = options.keyProducer_;
			valueProducer_ = options.valueProducer_;

			DatabaseWrapper databaseWrapper = options.databaseWrapperFactory_->open(options, instanceNumber_);
			db_ = (dynamic_cast<HashdbWapper*>(databaseWrapper.get()))->database();
		}

		virtual void run()
		{
			ProgressReporter reporter(iterations_);

			std::string key;
			
			BenchmarkReadBatch readBatch;
			readBatch.reserve(batchSize_);

			while (sequence_->hasNext()) {
				const size_type number = sequence_->next();

				keyProducer_->dataFor(key, number);
				readBatch.add(number, key);

				if (readBatch.count() >= batchSize_ || ! sequence_->hasNext()) {
					db_->fetch(readBatch);
					validateReadBatch(readBatch);
					readBatch.clear();
					readBatch.reserve(batchSize_);
				}

				reporter.next();
			}
			
			db_->close();
			reporter.finish();
		}

		virtual size_type instance()
		{
			return instanceNumber_;
		}

	private:
		size_type instanceNumber_;
		size_type iterations_;
		size_type batchSize_;
		Sequence sequence_;
		Database db_;
		DataProducer keyProducer_;
		DataProducer valueProducer_;
	};

	class BatchReadBenchmarkFactory : public IBenchmarkFactory {
	public:
		virtual Benchmark newBenchmark(size_type instanceNumber /* = 0 */)
		{
			Benchmark newInstance(new BatchReadBenchmark(instanceNumber));
			return newInstance;
		}

		virtual std::string name() const
		{
			return "batchRead";
		}

		virtual bool isBatch()
		{
			return true;
		}

		virtual SequenceFactory defaultSequenceFactory()
		{
			return newSimpleSequenceFactory(0, DEFAULT_READ_ITERATIONS);
		}
	};

	BenchmarkFactory newBatchReadBenchmarkFactory()
	{
		BenchmarkFactory newInstance(new BatchReadBenchmarkFactory());
		return newInstance;
	}

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
