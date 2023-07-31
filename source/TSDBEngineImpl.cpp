//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include <optional>

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
    auto it = _tables.find(tableName);
    if (it != _tables.cend()) {
        ERR_LOG("The table [%s] existed, cannot create as a new table", tableName.c_str())
        return -1;
    }
    TableSPtr table = std::make_shared<Table>();
    table->_table_name = tableName;
    table->_table_schema = std::make_shared<TableSchema>(schema);
    table->_table_writer = std::make_unique<TableWriter>(table->_table_name, table->_table_schema);
    table->_table_reader = std::make_unique<TableReader>();
    _tables.emplace(tableName, table);
    INFO_LOG("Created new table [%s]", tableName.c_str())
    return 0;
}

int TSDBEngineImpl::shutdown() {
    return 0;
}

int TSDBEngineImpl::upsert(const WriteRequest &writeRequest) {
    auto it = _tables.find(writeRequest.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", writeRequest.tableName.c_str())
        return -1;
    }
    TableSPtr table = _tables[writeRequest.tableName];
    std::optional<SegmentSPtr> segment_data = table->_table_writer->append(writeRequest);
    if (segment_data.has_value()) {
        table->_table_data.emplace_back(*segment_data);
    }
    return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
    auto it = _tables.find(pReadReq.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", pReadReq.tableName.c_str())
        return -1;
    }
    for (const auto& vin : pReadReq.vins) {
        std::string key(vin.vin, 17);
        TableSPtr table = it->second;
        PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(pReadReq.requestedColumns));
        table->_table_reader->init(table->_table_data, pReadReq.tableName, partial_schema, key);
        pReadRes.emplace_back(std::move(table->_table_reader->execute_latest_query(pReadReq)));
        table->_table_reader->reset();
    }
    return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
    auto it = _tables.find(trReadReq.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", trReadReq.tableName.c_str())
        return -1;
    }
    std::string key(trReadReq.vin.vin, 17);
    TableSPtr table = it->second;
    PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(trReadReq.requestedColumns));
    table->_table_reader->init(table->_table_data, trReadReq.tableName, partial_schema, key);
    trReadRes = std::move(table->_table_reader->execute_time_range_query(trReadReq));
    table->_table_reader->reset();
    return 0;
}

}