//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"

namespace LindormContest {

static std::string column_type_to_string(ColumnType type) {
    switch (type) {
    case COLUMN_TYPE_INTEGER:
        return "COLUMN_TYPE_INTEGER";
    case COLUMN_TYPE_TIMESTAMP:
        return "COLUMN_TYPE_TIMESTAMP";
    case COLUMN_TYPE_DOUBLE_FLOAT:
        return "COLUMN_TYPE_DOUBLE_FLOAT";
    case COLUMN_TYPE_STRING:
        return "COLUMN_TYPE_STRING";
    case COLUMN_TYPE_UNINITIALIZED:
        return "COLUMN_TYPE_UNINITIALIZED";
    }
    return "COLUMN_TYPE_UNINITIALIZED";
}

static ColumnType string_to_column_type(std::string s) {
    if (s == "COLUMN_TYPE_INTEGER") {
        return COLUMN_TYPE_INTEGER;
    }
    if (s == "COLUMN_TYPE_TIMESTAMP") {
        return COLUMN_TYPE_TIMESTAMP;
    }
    if (s == "COLUMN_TYPE_DOUBLE_FLOAT") {
        return COLUMN_TYPE_DOUBLE_FLOAT;
    }
    if (s == "COLUMN_TYPE_STRING") {
        return COLUMN_TYPE_STRING;
    }
    return COLUMN_TYPE_UNINITIALIZED;
}

static void save_schema_to_file(TableSchemaSPtr table_schema, std::string file_path) {
    std::ofstream output_file(file_path);

    for (const auto& column : table_schema->columns()) {
        if (column.get_name() == "vin" || column.get_name() == "timestamp") {
            continue;
        }
        output_file << column.get_name() << " " << column_type_to_string(column.get_column_type()) << std::endl;
    }

    output_file.close();
}

static TableSchemaSPtr load_schema_from_file(std::string file_path) {
    std::map<std::string, ColumnType> column_type_map;
    std::ifstream input_file(file_path);

    if (input_file.is_open()) {
        std::string column_name;
        std::string column_type_str;

        while (input_file >> column_name >> column_type_str) {
            ColumnType column_type = string_to_column_type(column_type_str);
            column_type_map[column_name] = column_type;
        }

        input_file.close();
    } else {
        ERR_LOG("Error opening schema file")
        throw std::runtime_error("Error opening schema file");
    }

    Schema schema;
    schema.columnTypeMap = std::move(column_type_map);
    return std::make_shared<TableSchema>(schema);
}

static void save_next_segment_id_to_file(size_t next_segment_id, std::string file_path) {
    std::ofstream output_file(file_path);
    output_file << next_segment_id << std::endl;
    output_file.close();
}

static size_t load_next_segment_id_from_file(std::string file_path) {
    std::ifstream input_file(file_path);
    if (input_file.is_open()) {
        std::string next_segment_id_str;
        input_file >> next_segment_id_str;
        input_file.close();
        return std::stoi(next_segment_id_str);
    } else {
        ERR_LOG("Error opening next_segment_id file")
        throw std::runtime_error("Error opening next_segment_id file");
    }
}

/**
 * This constructor's function signature should not be modified.
 * Our evaluation program will call this constructor.
 * The function's body can be modified.
 */
TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
        : TSDBEngine(dataDirPath) {
    _fs = io::FileSystem::create(dataDirPath);
    INFO_LOG("TSDBEngine created success")
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
        table->_next_segment_id = load_next_segment_id_from_file(table_path / "next_segment_id");
        table->_table_writer = std::make_unique<TableWriter>(table->_fs, table->_table_schema, &table->_next_segment_id, MEM_TABLE_FLUSH_THRESHOLD);
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
    table->_next_segment_id = 0;
    table->_table_schema = std::make_shared<TableSchema>(schema);
    table->_table_writer = std::make_unique<TableWriter>(table->_fs, table->_table_schema, &table->_next_segment_id, MEM_TABLE_FLUSH_THRESHOLD);
    table->_table_reader = std::make_unique<TableReader>(table->_fs, table->_table_schema);
    _tables.emplace(tableName, table);
    INFO_LOG("Created new table [%s]", tableName.c_str())
    return 0;
}

int TSDBEngineImpl::shutdown() {
    for (const auto& table : _tables) {
        table.second->_table_writer->close();
        io::Path schema_path = dataDirPath / io::Path(table.first) / io::Path("schema.txt");
        io::Path next_segment_id_path = dataDirPath / io::Path(table.first) / io::Path("next_segment_id");
        save_schema_to_file(table.second->_table_schema, schema_path);
        save_next_segment_id_to_file(table.second->_next_segment_id.load(), next_segment_id_path);
    }
    INFO_LOG("TSDBEngine shutdown success")
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
        ERR_LOG("No such table [%s], cannot execute latest query", pReadReq.tableName.c_str())
        return -1;
    }
    {
        std::unique_lock<std::mutex> l(_latch);
        TableSPtr table = it->second;
        if (table->_table_writer->rows() != 0) {
            table->_table_writer->flush();
            table->_table_reader->init_segment_readers();
        }
        PartialSchemaSPtr schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(pReadReq.requestedColumns));
        table->_table_reader->handle_latest_query(schema, pReadReq.vins, pReadRes);
    }
    return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
    auto it = _tables.find(trReadReq.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot execute time range query", trReadReq.tableName.c_str())
        return -1;
    }
    {
        std::unique_lock<std::mutex> l(_latch);
        TableSPtr table = it->second;
        if (table->_table_writer->rows() != 0) {
            table->_table_writer->flush();
            table->_table_reader->init_segment_readers();
        }
        PartialSchemaSPtr schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(trReadReq.requestedColumns));
        table->_table_reader->handle_time_range_query(schema, trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound, trReadRes);
    }
    return 0;
}

}