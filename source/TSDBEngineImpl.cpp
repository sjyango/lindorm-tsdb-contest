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
        : TSDBEngine(dataDirPath) {
    _fs = io::FileSystem::create(dataDirPath);
}

TSDBEngineImpl::~TSDBEngineImpl() = default;

int TSDBEngineImpl::connect() {
    if (_connected) {
        ERR_LOG("TSDB Engine has been connected")
        return -1;
    }

    std::vector<io::FileInfo> file_infos;
    _fs->list(dataDirPath, false, &file_infos);

    for (const auto& file_info : file_infos) {
        io::Path table_name = file_info._file_name;
        io::Path table_path = dataDirPath / table_name;
        TableSPtr table = std::make_shared<Table>();
        table->_fs = io::FileSystem::create(table_path);
        table->_table_schema = load_schema_from_file(table_path / "schema.txt");
        table->_table_writer = std::make_unique<TableWriter>(table->_fs, table->_table_schema, MEM_TABLE_FLUSH_THRESHOLD);
        table->_table_reader = std::make_unique<TableReader>(table->_fs, table->_table_schema);
        _tables.emplace(table_name, table);
        INFO_LOG("Load table [%s]", table_name.c_str())
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
    io::Path table_path = dataDirPath / io::Path(tableName);
    _fs->create_directory(table_path);
    table->_fs = io::FileSystem::create(table_path);
    table->_table_schema = std::make_shared<TableSchema>(schema);
    table->_table_writer = std::make_unique<TableWriter>(table->_fs, table->_table_schema, MEM_TABLE_FLUSH_THRESHOLD);
    table->_table_reader = std::make_unique<TableReader>(table->_fs, table->_table_schema);
    _tables.emplace(tableName, table);
    INFO_LOG("Created new table [%s]", tableName.c_str())
    return 0;
}

int TSDBEngineImpl::shutdown() {
    for (const auto& table : _tables) {
        table.second->_table_writer->close();
        io::Path schema_path = dataDirPath / io::Path(table.first) / io::Path("schema.txt");
        save_schema_to_file(table.second->_table_schema, schema_path);
    }
    return 0;
}

int TSDBEngineImpl::upsert(const WriteRequest &writeRequest) {
    auto it = _tables.find(writeRequest.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", writeRequest.tableName.c_str())
        return -1;
    }
    TableSPtr table = _tables[writeRequest.tableName];
    table->_table_writer->append(writeRequest.rows);
    return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
    auto it = _tables.find(pReadReq.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", pReadReq.tableName.c_str())
        return -1;
    }
    TableSPtr table = it->second;
    PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(pReadReq.requestedColumns));
    table->_table_writer->flush();
    table->_table_reader->init(partial_schema);
    table->_table_reader->handle_latest_query(pReadReq.vins, pReadRes);
    table->_table_reader->reset();
    return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
    auto it = _tables.find(trReadReq.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", trReadReq.tableName.c_str())
        return -1;
    }
    TableSPtr table = it->second;
    PartialSchemaSPtr partial_schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(trReadReq.requestedColumns));
    table->_table_writer->flush();
    table->_table_reader->init(partial_schema);
    table->_table_reader->handle_time_range_query(trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound, trReadRes);
    table->_table_reader->reset();
    return 0;
}

}