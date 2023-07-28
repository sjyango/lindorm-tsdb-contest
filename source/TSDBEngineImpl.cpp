//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"

namespace LindormContest {

/**
 * This constructor's function signature should not be modified.
 * Our evaluation program will call this constructor.
 * The function's body can be modified.
 */
TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
        : TSDBEngine(dataDirPath) {}

TSDBEngineImpl::~TSDBEngineImpl() = default;

int TSDBEngineImpl::connect() {
    if (_connected) {
        ERR_LOG("TSDB Engine has been connected")
        return -1;
    }


    _connected = true;
    INFO_LOG("TSDB Engine connected. Data path: [%s]", dataDirPath.c_str())
    return 0;
}

int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
    auto it = _schemas.find(tableName);
    if (it != _schemas.cend()) {
        ERR_LOG("The table [%s] existed, cannot create as a new table", tableName.c_str())
        return -1;
    }
    _schemas.insert({tableName, schema});
    _delta_writers.insert({tableName, storage::DeltaWriter::open(dataDirPath, tableName, schema, &_segment_datas)});
    INFO_LOG("Created new table [%s]", tableName.c_str())
    return 0;
}

int TSDBEngineImpl::shutdown() {
    return 0;
}

int TSDBEngineImpl::upsert(const WriteRequest &writeRequest) {
    auto it = _delta_writers.find(writeRequest.tableName);
    if (it == _delta_writers.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", writeRequest.tableName.c_str())
        return -1;
    }
    auto& delta_writer = _delta_writers[writeRequest.tableName];
    delta_writer->append(writeRequest);
    return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
    return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
    return 0;
}

}