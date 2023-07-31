//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#pragma once

#include <unordered_map>

#include "Root.h"
#include "TSDBEngine.hpp"
#include "storage/segment_traits.h"
#include "storage/table_writer.h"
#include "storage/table_reader.h"
#include "struct/Schema.h"

namespace LindormContest {

using namespace storage;

struct Table;

using TableSPtr = std::shared_ptr<Table>;

struct Table {
    std::string _table_name;
    TableSchemaSPtr _table_schema;
    std::unique_ptr<TableWriter> _table_writer;
    std::unique_ptr<TableReader> _table_reader;
    std::vector<SegmentSPtr> _table_data;
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
    bool _connected = false;
    std::unordered_map<std::string, TableSPtr> _tables;
}; // End class TSDBEngineImpl.

}
