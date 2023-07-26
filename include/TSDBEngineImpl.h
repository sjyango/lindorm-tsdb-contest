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
#include "storage/delta_writer.h"

namespace LindormContest {

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
    std::unordered_map<std::string, Schema> _schemas; // table_name -> schema
    std::unordered_map<std::string, std::unique_ptr<storage::DeltaWriter>> _delta_writers; // table_name -> delta_writer
}; // End class TSDBEngineImpl.

}
