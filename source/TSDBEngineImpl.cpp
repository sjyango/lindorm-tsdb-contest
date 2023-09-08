//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"
#include <fstream>

namespace LindormContest {

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath) {
        _shard_mem_map.set_root_path(_get_root_path());
    }

    TSDBEngineImpl::~TSDBEngineImpl() = default;

    int TSDBEngineImpl::connect() {
        _load_schema_from_file();
        return 0;
    }

    int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
        _schema = std::make_shared<Schema>(schema);
        _shard_mem_map.set_schema(_schema);
        return 0;
    }

    int TSDBEngineImpl::shutdown() {
        return 0;
    }

    int TSDBEngineImpl::write(const WriteRequest &writeRequest) {
        for (const auto &row: writeRequest.rows) {
            _latest_manager.add_latest(row);
            _shard_mem_map.append(row);
        }
        return 0;
    }

    int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        for (const auto &vin: pReadReq.vins) {
            pReadRes.emplace_back(_latest_manager.get_latest(vin, pReadReq.requestedColumns));
        }
        return 0;
    }

    int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
        return 0;
    }

    int TSDBEngineImpl::executeAggregateQuery(const TimeRangeAggregationRequest &aggregationReq,
                                              std::vector<Row> &aggregationRes) {
        return 0;
    }

    int TSDBEngineImpl::executeDownsampleQuery(const TimeRangeDownsampleRequest &downsampleReq,
                                               std::vector<Row> &downsampleRes) {
        return 0;
    }

    void TSDBEngineImpl::_save_schema_to_file() {
        std::ofstream schema_out;
        Path schema_path = _get_root_path() / "schema.txt";
        schema_out.open(schema_path, std::ios::out);
        schema_out << (uint8_t) _schema->columnTypeMap.size() << " ";

        for (const auto & [column_name, column_type]: _schema->columnTypeMap) {
            schema_out << column_name << " ";
            schema_out << (uint8_t) column_type << " ";
        }

        schema_out.close();
    }

    void TSDBEngineImpl::_load_schema_from_file() {
        std::ifstream schema_fin;
        Path schema_path = _get_root_path() / "schema.txt";
        schema_fin.open(schema_path, std::ios::in);
        if (!schema_fin.is_open() || !schema_fin.good()) {
            schema_fin.close();
            INFO_LOG("schema.txt doesn't exist!")
            return;
        }

        std::map<std::string, ColumnType> column_type_map;
        uint8_t column_nums;
        schema_fin >> column_nums;
        if (column_nums <= 0) {
            schema_fin.close();
            throw std::runtime_error("unexpected columns' num");
        }

        for (int i = 0; i < column_nums; ++i) {
            std::string column_name;
            uint8_t column_type_int;
            schema_fin >> column_name;
            schema_fin >> column_type_int;
            column_type_map.emplace(column_name, (ColumnType) column_type_int);
        }

        _schema = std::make_shared<Schema>(std::move(column_type_map));
    }
}