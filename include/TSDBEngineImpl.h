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
    std::unordered_map<int32_t, Row> _latest_records; // vin -> {segment_id, ordinal, latest_timestamp}
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
};

static void update_latest_records(std::unordered_map<int32_t, Row>& latest_records,
                                  const std::unordered_map<int32_t, Row>& new_records) {
    for (const auto& record : new_records) {
        if (latest_records.find(record.first) == latest_records.end()) {
            latest_records.emplace(record);
        } else {
            if (record.second.timestamp > latest_records[record.first].timestamp) {
                latest_records[record.first] = record.second;
            }
        }
    }
}

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

static std::string serialize_row(const TableSchema& schema, Row row) {
    std::string dst;
    int32_t vin_val = decode_vin(row.vin);
    char vin_buf[sizeof(vin_val)];
    std::memcpy(vin_buf, &vin_val, sizeof(vin_val));
    dst.append(vin_buf, sizeof(vin_buf));
    uint16_t timestamp_val = decode_timestamp(row.timestamp);
    char timestamp_buf[sizeof(timestamp_val)];
    std::memcpy(timestamp_buf, &timestamp_val, sizeof(timestamp_val));
    dst.append(timestamp_buf, sizeof(timestamp_buf));

    for (const auto& column : schema.columns()) {
        if (column.get_name() == "vin" || column.get_name() == "timestamp") {
            continue;
        }
        ColumnValue column_value = row.columns[column.get_name()];
        switch (column_value.columnType) {
        case COLUMN_TYPE_INTEGER: {
            int32_t int_val;
            column_value.getIntegerValue(int_val);
            char int_buf[sizeof(int_val)];
            std::memcpy(int_buf, &int_val, sizeof(int_val));
            dst.append(int_buf, sizeof(int_buf));
            break;
        }
        case COLUMN_TYPE_DOUBLE_FLOAT: {
            double_t float_val;
            column_value.getDoubleFloatValue(float_val);
            char float_buf[sizeof(float_val)];
            std::memcpy(float_buf, &float_val, sizeof(float_val));
            dst.append(float_buf, sizeof(float_buf));
            break;
        }
        case COLUMN_TYPE_STRING: {
            std::pair<int32_t, const char *> str_val;
            column_value.getStringValue(str_val);
            put_fixed32_le(&dst, str_val.first);
            dst.append(str_val.second, str_val.first);
            break;
        }
        default: {
            // do nothing
        }
        }
    }
    return std::move(dst);
}

static Row deserialize_row(const TableSchema& schema, const char* src_ptr, size_t src_length) {
    Row row;
    size_t src_offset = 0;
    row.vin = encode_vin(*reinterpret_cast<const int32_t*>(src_ptr + src_offset));
    src_offset += sizeof(int32_t);
    row.timestamp = encode_timestamp(*reinterpret_cast<const uint16_t*>(src_ptr + src_offset));
    src_offset += sizeof(uint16_t);
    std::map<std::string, ColumnValue> columns;

    for (const auto& column : schema.columns()) {
        if (column.get_name() == "vin" || column.get_name() == "timestamp") {
            continue;
        }
        switch (column.get_column_type()) {
        case COLUMN_TYPE_INTEGER: {
            int32_t int_val = *reinterpret_cast<const int32_t*>(src_ptr + src_offset);
            src_offset += sizeof(int32_t);
            columns.emplace(column.get_name(), ColumnValue(int_val));
            break;
        }
        case COLUMN_TYPE_DOUBLE_FLOAT: {
            double_t float_val = *reinterpret_cast<const double_t*>(src_ptr + src_offset);
            src_offset += sizeof(double_t);
            columns.emplace(column.get_name(), ColumnValue(float_val));
            break;
        }
        case COLUMN_TYPE_STRING: {
            uint32_t str_length = *reinterpret_cast<const uint32_t*>(src_ptr + src_offset);
            src_offset += sizeof(uint32_t);
            columns.emplace(column.get_name(), ColumnValue(src_ptr + src_offset, str_length));
            src_offset += str_length;
            break;
        }
        default: {
            // do nothing
        }
        }
    }

    assert(src_offset == src_length);
    row.columns = std::move(columns);
    return std::move(row);
}

static void save_latest_records_to_file(TableSchemaSPtr schema, const std::unordered_map<int32_t, Row>& latest_records, std::string file_path) {
    std::ofstream output_file(file_path, std::ios::out | std::ios::binary);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open file for writing." << std::endl;
        return;
    }
    uint32_t record_nums = latest_records.size();
    output_file.write(reinterpret_cast<const char*>(&record_nums), sizeof(uint32_t));

    for (const auto& entry : latest_records) {
        std::string row_str = serialize_row(*schema, entry.second);
        uint32_t row_str_length = row_str.size();
        output_file.write(reinterpret_cast<const char*>(&entry.first), sizeof(entry.first));
        output_file.write(reinterpret_cast<const char*>(&row_str_length), sizeof(uint32_t));
        output_file.write(row_str.c_str(), row_str_length);
    }

    output_file.close();
}

static std::unordered_map<int32_t, Row> load_latest_records_from_file(TableSchemaSPtr schema, std::string file_path) {
    std::unordered_map<int32_t, Row> latest_records;

    std::ifstream input_file(file_path, std::ios::in | std::ios::binary);
    if (!input_file.is_open()) {
        std::cerr << "Failed to open file for reading." << std::endl;
        return latest_records;
    }
    std::string content((std::istreambuf_iterator<char>(input_file)),
                        (std::istreambuf_iterator<char>()));
    const char* content_ptr = content.c_str();
    size_t content_offset = 0;
    uint32_t record_nums = *reinterpret_cast<const uint32_t*>(content_ptr);
    content_offset += sizeof(uint32_t);

    for (uint32_t i = 0; i < record_nums; ++i) {
        int32_t vin_val = *reinterpret_cast<const int32_t*>(content_ptr + content_offset);
        content_offset += sizeof(int32_t);
        uint32_t row_str_length = *reinterpret_cast<const uint32_t*>(content_ptr + content_offset);
        content_offset += sizeof(uint32_t);
        Row row = std::move(deserialize_row(*schema, content_ptr + content_offset, row_str_length));
        content_offset += row_str_length;
        latest_records[vin_val] = row;
    }

    assert(content_offset == content.size());
    input_file.close();
    return latest_records;
}

}
