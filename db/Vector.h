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

// Vector.h -- vector whose StaticCapacity entries reside on stack.
#pragma once
#include <boost/container/vector.hpp>
#include <boost/container/static_vector.hpp>
#include <kerio/hashdb/Constants.h>
#include "utils/Assert.h"

namespace kerio {
namespace hashdb {

	static const size_type VECTOR_DEFAULT_STATIC_CAPACITY = 16;

	template <class IterParentType, class IterValueType>
	class VectorIterator {
	public:
		typedef typename boost::mpl::if_<typename boost::is_const<IterValueType>::type, const IterParentType, IterParentType>::type parent_type;
		typedef typename IterParentType::value_type value_type;
		typedef IterValueType& reference;
		typedef IterValueType* pointer;

		typedef std::random_access_iterator_tag iterator_category;
		typedef std::ptrdiff_t difference_type;

		VectorIterator()
			: parent_(NULL)
			, index_(0)
		{

		}

		explicit VectorIterator(parent_type* parent, size_type index)
			: parent_(parent)
			, index_(index)
		{

		}

		VectorIterator(const VectorIterator& other)
			: parent_(other.parent_)
			, index_(other.index_)
		{

		}

		VectorIterator& operator=(const VectorIterator& other)
		{
			if (this != &other) {
				parent_ = other.parent_;
				index_ = other.index_;
			}

			return *this;
		}

		bool operator==(const VectorIterator& other) const
		{
			HASHDB_ASSERT(parent_ == other.parent_);
			return index_ == other.index_;
		}

		bool operator!=(const VectorIterator& other) const
		{
			return ! this->operator==(other);
		}

		bool operator<(const VectorIterator& other) const
		{
			HASHDB_ASSERT(parent_ == other.parent_);
			return index_ < other.index_;
		}

		VectorIterator& operator++() // Prefix.
		{
			++index_;
			return *this;
		}

		VectorIterator operator++(int) // Postfix.
		{
			VectorIterator rv(*this);
			++index_;
			return rv;
		}

		VectorIterator& operator--() // Prefix.
		{
			--index_;
			return *this;
		}

		VectorIterator operator--(int) // Postfix.
		{
			VectorIterator rv(*this);
			--index_;
			return rv;
		}

		VectorIterator& operator+=(size_t increment)
		{
			index_ += static_cast<size_type>(increment);
			return *this;
		}

		VectorIterator operator+(size_t increment) const
		{
			VectorIterator rv(*this);
			rv += increment;
			return rv;
		}

		VectorIterator& operator-=(size_t decrement)
		{
			index_ -= static_cast<size_type>(decrement);
			return *this;
		}

		VectorIterator operator-(size_t decrement) const
		{
			VectorIterator rv(*this);
			rv -= decrement;
			return rv;
		}

		difference_type operator-(const VectorIterator& other) const
		{
			HASHDB_ASSERT(parent_ == other.parent_);
			return index_ - other.index_;
		}

		reference operator*() const
		{
			return (*parent_)[index_];
		}

		pointer operator->() const
		{
			return &((*parent_)[index_]);
		}

		reference operator[](size_t offset) const
		{
			const size_type newIndex = index_ + static_cast<size_type>(offset);
			return (*parent_)[newIndex];
		}

		parent_type* parent()
		{
			return parent_;
		}

		size_type index()
		{
			return index_;
		}

	private:
		parent_type* parent_;
		size_type index_;
	};



	template<class ValueType, std::size_t StaticCapacity = VECTOR_DEFAULT_STATIC_CAPACITY>
	class Vector {	
	public:
		typedef kerio::hashdb::size_type size_type;
		static const size_type npos = SIZE_TYPE_MAX;

		typedef Vector<ValueType, StaticCapacity> vector_type;
		typedef ValueType value_type;
		static const size_type static_capacity = static_cast<size_type>(StaticCapacity);

		typedef typename kerio::hashdb::VectorIterator<vector_type, value_type> iterator;
		typedef typename kerio::hashdb::VectorIterator<vector_type, value_type const> const_iterator;

		void reserve(size_type n)
		{
			if (n > capacity()) {
				dynamicVector_.reserve(n - static_capacity);
			}
		}

		size_type capacity() const
		{
			return static_capacity + static_cast<size_type>(dynamicVector_.capacity());
		}

		size_type size() const
		{
			const size_t rv = staticVector_.size() + dynamicVector_.size();
			return static_cast<size_type>(rv);
		}

		bool empty() const
		{	
			return staticVector_.empty();
		}

		void resize(size_type newSize)
		{
			const size_type staticSize = (newSize < static_capacity)? newSize : static_capacity;
			const size_type dynamicSize = newSize - staticSize;

			staticVector_.resize(staticSize);
			dynamicVector_.resize(dynamicSize);
		}

		const value_type& operator[](size_type pos) const
		{
			HASHDB_ASSERT(isValidIndex(pos));
			return (pos < static_capacity)? staticVector_[pos] : dynamicVector_[pos - static_capacity];
		}

		value_type& operator[](size_type pos)
		{
			HASHDB_ASSERT(isValidIndex(pos));
			return (pos < static_capacity)? staticVector_[pos] : dynamicVector_[pos - static_capacity];
		}

		const value_type& front() const
		{	
			return staticVector_.front();
		}

		value_type& front()
		{	
			return staticVector_.front();
		}

		const value_type& back() const
		{	
			return (dynamicVector_.empty())? staticVector_.back() : dynamicVector_.back();
		}

		value_type& back()
		{	
			return (dynamicVector_.empty())? staticVector_.back() : dynamicVector_.back();
		}

		void push_back(const value_type& val)
		{	
			if (staticVector_.size() < static_capacity) {
				staticVector_.push_back(val);
			}
			else {
				dynamicVector_.push_back(val);
			}
		}

		template<class ArgT>
		value_type& emplace_back(const ArgT& arg)
		{
			if (staticVector_.size() < static_capacity) {
				staticVector_.emplace_back(arg);
			}
			else {
				dynamicVector_.emplace_back(arg);
			}

			return back();
		}

		template<class ArgT1, class ArgT2>
		value_type& emplace_back(const ArgT1& arg1, const ArgT2& arg2)
		{
			if (staticVector_.size() < static_capacity) {
				staticVector_.emplace_back(arg1, arg2);
			}
			else {
				dynamicVector_.emplace_back(arg1, arg2);
			}

			return back();
		}

		void clear()
		{	
			staticVector_.clear();
			dynamicVector_.clear();
		}

		const_iterator begin() const
		{	
			return const_iterator(this, 0);
		}

		iterator begin()
		{	
			return iterator(this, 0);
		}

		iterator end() const
		{	
			return const_iterator(this, size());
		}

		iterator end()
		{	
			return iterator(this, size());
		}

		size_type find(const value_type& val) const
		{
			const size_type count = size();

			size_type pos = npos;
			for (size_type i = 0; i < count; ++i) {
				if (operator[](i) == val) {
					pos = i;
					break;
				}
			}

			return pos;
		}

		bool contains(const value_type& val) const
		{
			return find(val) != npos;
		}

		void sort()
		{
			std::sort(begin(), end());
		}

	private:
		bool isValidIndex(size_type pos) const
		{
			return (pos < static_capacity)? (pos < staticVector_.size()) : ((pos - static_capacity) < dynamicVector_.size());
		}

		boost::container::static_vector<value_type, StaticCapacity> staticVector_;
		boost::container::vector<value_type> dynamicVector_;
	};

}; // namespace hashdb
}; // namespace kerio
