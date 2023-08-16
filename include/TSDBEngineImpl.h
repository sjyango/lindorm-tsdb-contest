//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#pragma once

#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <atomic>

#include "Root.h"
#include "TSDBEngine.hpp"
#include "storage/segment_traits.h"
#include "storage/table_writer.h"
#include "storage/table_reader.h"
#include "storage/table_schema.h"
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
        throw std::runtime_error("Error opening schema file");
    }
}

}
