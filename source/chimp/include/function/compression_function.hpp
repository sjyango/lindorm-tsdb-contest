//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/compression_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
class DatabaseInstance;
class ColumnData;
class ColumnDataCheckpointer;
class ColumnSegment;
class SegmentStatistics;

struct ColumnFetchState;
struct ColumnScanState;
struct SegmentScanState;

struct AnalyzeState {
	virtual ~AnalyzeState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct CompressionState {
	virtual ~CompressionState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct CompressedSegmentState {
	virtual ~CompressedSegmentState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct SegmentScanState {
        virtual ~SegmentScanState() {
        }
        
        template <class TARGET>
        TARGET &Cast() {
                D_ASSERT(dynamic_cast<TARGET *>(this));
                return reinterpret_cast<TARGET &>(*this);
        }
        template <class TARGET>
        const TARGET &Cast() const {
                D_ASSERT(dynamic_cast<const TARGET *>(this));
                return reinterpret_cast<const TARGET &>(*this);
        }
};
} // namespace duckdb
