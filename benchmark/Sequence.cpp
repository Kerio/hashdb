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

#if defined _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4724)      // potential mod by 0
#endif // defined _MSC_VER

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#if defined _MSC_VER
#pragma warning(pop)
#endif // defined _MSC_VER

#include "Sequence.h"

namespace kerio {
namespace hashdb {
namespace benchmark {

	//----------------------------------------------------------------------------

	class RandomSequence : public ISequence {
	public:
		RandomSequence(size_type maxResult, size_type sequenceSize, size_type seed)
			: generator_(seed)
			, distribution_(0, maxResult)
			, sequenceNumber_(0)
			, sequenceSize_(sequenceSize)
		{

		}

		virtual size_type next()
		{
			++sequenceNumber_;
			return distribution_(generator_);
		}

		virtual bool hasNext() const
		{
			return sequenceNumber_ != sequenceSize_;
		}

		virtual ~RandomSequence() { }

	private:
		boost::random::mt19937 generator_;
		boost::random::uniform_int_distribution<> distribution_;
		size_type sequenceNumber_;
		const size_type sequenceSize_;
	};

	class RandomSequenceFactory : public ISequenceFactory {
	public:
		RandomSequenceFactory(size_type maxResult, size_type sequenceSize)
			: maxResult_(maxResult)
			, sequenceSize_(sequenceSize)
			, seed_(0)
		{

		}

		virtual Sequence newSequence()
		{
			Sequence newInstance(new RandomSequence(maxResult_, sequenceSize_, seed_));
			++seed_;
			return newInstance;
		}

		virtual std::string name() const
		{
			std::ostringstream os;
			os << "random(0.." << maxResult_ << ")";
			return os.str();
		}

		virtual size_type size() const
		{
			return sequenceSize_;
		}

		virtual ~RandomSequenceFactory() { }

	private:
		const size_type maxResult_;
		const size_type sequenceSize_;
		volatile size_type seed_;
	};

	SequenceFactory newRandomSequenceFactory(size_type maxResult, size_type sequenceSize)
	{
		SequenceFactory newFactory(new RandomSequenceFactory(maxResult, sequenceSize));
		return newFactory;
	}

	//----------------------------------------------------------------------------

	class SimpleSequence : public ISequence {
	public:
		SimpleSequence(size_type initialValue, size_type endValue)
			: endValue_(endValue)
			, currentValue_(initialValue)
		{

		}

		virtual size_type next()
		{
			return currentValue_++;
		}

		virtual bool hasNext() const
		{
			return currentValue_ != endValue_;
		}

		virtual ~SimpleSequence() { }

	private:
		const size_type endValue_;
		size_type currentValue_;
	};

	class SimpleSequenceFactory : public ISequenceFactory {
	public:
		SimpleSequenceFactory(size_type initialValue, size_type sequenceSize)
			: initialValue_(initialValue)
			, endValue_(initialValue + sequenceSize)
		{

		}

		virtual Sequence newSequence()
		{
			Sequence newInstance(new SimpleSequence(initialValue_, endValue_));
			return newInstance;
		}

		virtual std::string name() const
		{
			std::ostringstream os;
			os << "sequence(" << initialValue_ << ".." << endValue_ - 1 << ")";
			return os.str();
		}

		virtual size_type size() const
		{
			return endValue_ - initialValue_;
		}

		virtual ~SimpleSequenceFactory() { }

	private:
		const size_type initialValue_;
		const size_type endValue_;
	};

	SequenceFactory newSimpleSequenceFactory(size_type initialValue, size_type sequenceSize)
	{
		SequenceFactory newFactory(new SimpleSequenceFactory(initialValue, sequenceSize));
		return newFactory;
	}

}; // namespace benchmark
}; // namespace hashdb
}; // namespace kerio
