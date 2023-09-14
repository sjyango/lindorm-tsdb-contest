//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"
#include <fstream>
#include <optional>

namespace LindormContest {

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath) {
        _index_manager = std::make_shared<GlobalIndexManager>();
        _thread_pool = std::make_shared<ThreadPool>(std::thread::hardware_concurrency());
        _writer_manager = std::make_unique<TsmWriterManager>(_index_manager, _thread_pool, _get_root_path());
        _latest_manager = std::make_unique<GlobalLatestManager>();
        _tr_manager = std::make_unique<GlobalTimeRangeManager>(_get_root_path(), _index_manager);
        _agg_manager = std::make_unique<GlobalAggregateManager>(_get_root_path(), _index_manager);
    }

    TSDBEngineImpl::~TSDBEngineImpl() = default;

    int TSDBEngineImpl::connect() {
        _load_schema_from_file();
        if (_schema == nullptr) {
            return 0;
        }
        _index_manager->decode_from_file(_get_root_path(), _schema);
        _latest_manager->load_latest_records_from_file(_get_latest_records_path(), _schema);
        _writer_manager->set_schema(_schema);
        _tr_manager->set_schema(_schema);
        _agg_manager->set_schema(_schema);
        return 0;
    }

    int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
        _schema = std::make_shared<Schema>(schema);
        _writer_manager->set_schema(_schema);
        _tr_manager->set_schema(_schema);
        _agg_manager->set_schema(_schema);
        return 0;
    }

    int TSDBEngineImpl::shutdown() {
        _save_schema_to_file();
        _latest_manager->save_latest_records_to_file(_get_latest_records_path(), _schema);
        _writer_manager->flush_all_async();
        _thread_pool->shutdown();
        return 0;
    }

    int TSDBEngineImpl::write(const WriteRequest &writeRequest) {
        for (const auto &row: writeRequest.rows) {
            _latest_manager->add_latest(row);
            _writer_manager->append(row);
        }
        return 0;
    }

    int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        for (const auto &vin: pReadReq.vins) {
            std::optional<Row> result = _latest_manager->get_latest(vin, pReadReq.requestedColumns);
            if (result.has_value()) {
                pReadRes.emplace_back(std::move(*result));
            }
        }
        return 0;
    }

    int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
        _tr_manager->query_time_range(trReadReq.vin,
                                      trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                      trReadReq.requestedColumns, trReadRes);
        return 0;
    }

    int TSDBEngineImpl::executeAggregateQuery(const TimeRangeAggregationRequest &aggregationReq,
                                              std::vector<Row> &aggregationRes) {
        _agg_manager->query_aggregate(aggregationReq.vin,
                                      aggregationReq.timeLowerBound, aggregationReq.timeUpperBound,
                                      aggregationReq.columnName, aggregationReq.aggregator, aggregationRes);
        return 0;
    }

    int TSDBEngineImpl::executeDownsampleQuery(const TimeRangeDownsampleRequest &downsampleReq,
                                               std::vector<Row> &downsampleRes) {
        return 0;
    }

    void TSDBEngineImpl::_save_schema_to_file() {
        std::ofstream schema_out;
        schema_out.open(_get_schema_path(), std::ios::out);

        for (const auto & [column_name, column_type]: _schema->columnTypeMap) {
            schema_out << column_name << " ";
            schema_out << (uint8_t) column_type << " ";
        }

        schema_out.close();
    }

    void TSDBEngineImpl::_load_schema_from_file() {
        std::ifstream schema_fin;
        schema_fin.open(_get_schema_path(), std::ios::in);
        if (!schema_fin.is_open() || !schema_fin.good()) {
            schema_fin.close();
            return;
        }
        std::map<std::string, ColumnType> column_type_map;

        for (int i = 0; i < SCHEMA_COLUMN_NUMS; ++i) {
            std::string column_name;
            uint8_t column_type_int;
            schema_fin >> column_name;
            schema_fin >> column_type_int;
            column_type_map.emplace(column_name, (ColumnType) column_type_int);
        }

        _schema = std::make_shared<Schema>(std::move(column_type_map));
    }
}