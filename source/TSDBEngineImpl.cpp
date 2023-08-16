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
        table->_latest_records = std::move(load_latest_records_from_file(table_path / "latest_records.dat"));
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
        auto flushed_records = std::move(table.second->_table_writer->close());
        if (flushed_records.has_value()) {
            for (const auto& record : (*flushed_records)) {
                if (table.second->_latest_records.find(record.first) == table.second->_latest_records.end()) {
                    table.second->_latest_records.emplace(record);
                } else {
                    if (record.second._timestamp > table.second->_latest_records[record.first]._timestamp) {
                        table.second->_latest_records[record.first] = record.second;
                    }
                }
            }

            INFO_LOG("_latest_records' size is %zu", table.second->_latest_records.size())
        }
        io::Path schema_path = dataDirPath / io::Path(table.first) / io::Path("schema.txt");
        io::Path next_segment_id_path = dataDirPath / io::Path(table.first) / io::Path("next_segment_id");
        io::Path latest_records_path = dataDirPath / io::Path(table.first) / io::Path("latest_records.dat");
        save_schema_to_file(table.second->_table_schema, schema_path);
        save_next_segment_id_to_file(table.second->_next_segment_id.load(), next_segment_id_path);
        save_latest_records_to_file(table.second->_latest_records, latest_records_path);
    }
    _tables.clear();
    INFO_LOG("TSDBEngine shutdown success")
    return 0;
}

int TSDBEngineImpl::upsert(const WriteRequest &writeRequest) {
    auto it = _tables.find(writeRequest.tableName);
    if (it == _tables.cend()) {
        ERR_LOG("No such table [%s], cannot upsert to", writeRequest.tableName.c_str())
        return -1;
    }
    {
        // std::unique_lock<std::mutex> l(_latch);
        TableSPtr table = _tables[writeRequest.tableName];
        std::optional<std::unordered_map<int32_t, RowPosition>> flushed_records;
        table->_table_writer->append(writeRequest.rows, &flushed_records);
        if (flushed_records.has_value()) {
            table->_table_reader->init_segment_readers();

            for (const auto& record : (*flushed_records)) {
                if (table->_latest_records.find(record.first) == table->_latest_records.end()) {
                    table->_latest_records.emplace(record);
                } else {
                    if (record.second._timestamp > table->_latest_records[record.first]._timestamp) {
                        table->_latest_records[record.first] = record.second;
                    }
                }
            }

            INFO_LOG("_latest_records' size is %zu", table->_latest_records.size())
        }
    }
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
            auto flushed_records = table->_table_writer->flush();
            if (flushed_records.has_value()) {
                table->_table_reader->init_segment_readers();

                for (const auto& record : (*flushed_records)) {
                    if (table->_latest_records.find(record.first) == table->_latest_records.end()) {
                        table->_latest_records.emplace(record);
                    } else {
                        if (record.second._timestamp > table->_latest_records[record.first]._timestamp) {
                            table->_latest_records[record.first] = record.second;
                        }
                    }
                }

                INFO_LOG("_latest_records' size is %zu", table->_latest_records.size())
            }
        }
        std::vector<RowPosition> row_positions;

        for (const auto& vin : pReadReq.vins) {
            int32_t vin_val = decode_vin(vin);
            if (table->_latest_records.find(vin_val) == table->_latest_records.end()) {
                continue;
            }
            row_positions.emplace_back(table->_latest_records[vin_val]);
        }

        PartialSchemaSPtr schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(pReadReq.requestedColumns));
        table->_table_reader->handle_latest_query(schema, row_positions, pReadRes);
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
            auto flushed_records = table->_table_writer->flush();
            if (flushed_records.has_value()) {
                table->_table_reader->init_segment_readers();

                for (const auto& record : (*flushed_records)) {
                    if (table->_latest_records.find(record.first) == table->_latest_records.end()) {
                        table->_latest_records.emplace(record);
                    } else {
                        if (record.second._timestamp > table->_latest_records[record.first]._timestamp) {
                            table->_latest_records[record.first] = record.second;
                        }
                    }
                }

                INFO_LOG("_latest_records' size is %zu", table->_latest_records.size())
            }
        }
        PartialSchemaSPtr schema = std::make_shared<PartialSchema>(table->_table_schema->column_by_names(trReadReq.requestedColumns));
        table->_table_reader->handle_time_range_query(schema, trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound, trReadRes);
    }
    return 0;
}

}