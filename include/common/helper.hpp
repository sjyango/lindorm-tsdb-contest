//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string.h>
#include <type_traits>

#ifdef _MSC_VER
#define suint64_t int64_t
#endif

#if defined(_WIN32) || defined(_WIN64)
#define DUCKDB_WINDOWS
#elif defined(__unix__) || defined(__unix) || (defined(__APPLE__) && defined(__MACH__))
#define DUCKDB_POSIX
#endif

namespace duckdb {

// explicit fallthrough for switch_statementss
#ifndef __has_cpp_attribute // For backwards compatibility
#define __has_cpp_attribute(x) 0
#endif
#if __has_cpp_attribute(clang::fallthrough)
#define DUCKDB_EXPLICIT_FALLTHROUGH [[clang::fallthrough]]
#elif __has_cpp_attribute(gnu::fallthrough)
#define DUCKDB_EXPLICIT_FALLTHROUGH [[gnu::fallthrough]]
#else
#define DUCKDB_EXPLICIT_FALLTHROUGH
#endif

template <class... T>
struct AlwaysFalse {
	static constexpr bool value = false;
};

template<typename T>
using reference = std::reference_wrapper<T>;


template <typename T>
T MaxValue(T a, T b) {
	return a > b ? a : b;
}

template <typename T>
T MinValue(T a, T b) {
	return a < b ? a : b;
}

template <typename T>
T AbsValue(T a) {
	return a < 0 ? -a : a;
}

//Align value (ceiling)
template<class T, T val=8>
static inline T AlignValue(T n) {
	return ((n + (val - 1)) / val) * val;
}

template<class T, T val=8>
static inline bool ValueIsAligned(T n) {
	return (n % val) == 0;
}

template <typename T>
T SignValue(T a) {
	return a < 0 ? -1 : 1;
}

template <typename T>
const T Load(const uint8_t *ptr) {
	T ret;
	memcpy(&ret, ptr, sizeof(ret));
	return ret;
}

template <typename T>
void Store(const T &val, uint8_t * ptr) {
	memcpy(ptr, (void *)&val, sizeof(val));
}

template<typename T>
using const_reference = std::reference_wrapper<const T>;

//! Returns whether or not two reference wrappers refer to the same object
template<class T>
bool RefersToSameObject(const reference<T> &A, const reference<T> &B) {
	return &A.get() == &B.get();
}

} // namespace duckdb
