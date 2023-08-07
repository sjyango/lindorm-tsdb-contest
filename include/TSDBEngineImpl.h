//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#pragma once

#include <unordered_map>
#include <mutex>
#include <atomic>

#include "Root.h"
#include "TSDBEngine.hpp"
#include "storage/segment_traits.h"
#include "storage/table_writer.h"
#include "storage/table_reader.h"
#include "struct/Schema.h"
#include "io/file_system.h"
#include "io/io_utils.h"

namespace LindormContest {

using namespace storage;

constexpr size_t MEM_TABLE_FLUSH_THRESHOLD = 1000000;

struct Table;

using TableSPtr = std::shared_ptr<Table>;

struct Table {
    io::FileSystemSPtr _fs;
    TableSchemaSPtr _table_schema;
    std::unique_ptr<TableWriter> _table_writer;
    std::unique_ptr<TableReader> _table_reader;
    std::atomic<size_t> _next_segment_id;
};

class TSDBEngineImpl : public TSDBEngine {
public:
    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    explicit TSDBEngineImpl(const std::string &dataDirPath);

    ~TSDBEngineImpl() override;

    int connect() override;

    int createTable(const std::string &tableName, const Schema &schema) override;

    int shutdown() override;

    int upsert(const WriteRequest &wReq) override;

    int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override;

    int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override;

private:
    std::mutex _latch;
    bool _connected = false;
    io::FileSystemSPtr _fs;
    std::unordered_map<std::string, TableSPtr> _tables;
}; // End class TSDBEngineImpl.

}
