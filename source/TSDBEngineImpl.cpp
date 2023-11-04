//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include <omp.h>

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
        Path compaction_data_path = _get_root_path() / "compaction";
        _finish_compaction = std::filesystem::exists(compaction_data_path);
        _convert_manager = std::make_shared<GlobalConvertManager>(_get_root_path());
        if (!_finish_compaction) {
            _writer_manager = std::make_unique<TsmWriterManager>(_get_root_path(), _convert_manager);
        }
        _index_manager = std::make_shared<GlobalIndexManager>();
        _latest_manager = std::make_unique<GlobalLatestManager>(_get_root_path(), _finish_compaction);
        _tr_manager = std::make_unique<GlobalTimeRangeManager>(_get_root_path(), _finish_compaction, _index_manager);
        _agg_manager = std::make_unique<GlobalAggregateManager>(_get_root_path(), _finish_compaction, _index_manager);
        _ds_manager = std::make_unique<GlobalDownSampleManager>(_get_root_path(), _finish_compaction, _index_manager);
    }

    TSDBEngineImpl::~TSDBEngineImpl() = default;

    int TSDBEngineImpl::connect() {
        _load_schema_from_file();
        if (_schema == nullptr) {
            return 0;
        }
        assert(_finish_compaction);
        _index_manager->decode_from_file(_get_root_path(), _schema);
        _latest_manager->init(_schema);
        _tr_manager->init(_schema);
        _agg_manager->init(_schema);
        _ds_manager->init(_schema);
        _convert_manager->init(_schema);
        _get_latest_records();
        return 0;
    }

    int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
        _schema = std::make_shared<Schema>(schema);
        _writer_manager->init(_schema);
        _latest_manager->init(_schema);
        _tr_manager->init(_schema);
        _agg_manager->init(_schema);
        _ds_manager->init(_schema);
        _convert_manager->init(_schema);
        return 0;
    }
    bool cmp(std::pair<std::string ,int> a,std::pair<std::string ,int> b){
        return a.second > b.second;
    }
    int TSDBEngineImpl::shutdown() {
        _save_schema_to_file();
        _convert_manager->finalize_convert();
        if (std::filesystem::exists(_get_root_path() / "no-compaction")) {
            std::filesystem::remove_all(_get_root_path() / "no-compaction");
        }
        INFO_LOG("####################### [demo->shutdown()] #######################")
        INFO_LOG("####map_size=%ld",string_map.size())
        int i = 0;
        std::vector<std::pair<std::string ,int>>vec(string_map.begin(),string_map.end());
        std::sort(vec.begin(),vec.end(),cmp);
        INFO_LOG("####size=%ld",vec.size())
        INFO_LOG("####firstone,key=%s,value=%d",vec[0].first.c_str(),vec[0].second)
        for(auto &e:vec){
            if(e.second>=2) {
                INFO_LOG("index=%d,key=%s,value=%d", i,e.first.c_str(), e.second)
                i++;
            }
        }
        return 0;
    }

    int TSDBEngineImpl::write(const WriteRequest &writeRequest) {
        for (const auto &row: writeRequest.rows) {
            _writer_manager->append(row,string_map);
        }
        return 0;
    }

    int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        for (const auto &vin: pReadReq.vins) {
            uint16_t vin_num = decode_vin(vin);
            if (unlikely(vin_num == INVALID_VIN_NUM)) {
                continue;
            }
            Row result_row;
            result_row.vin = vin;
            if (_finish_compaction) {
                _latest_manager->query_latest<true>(vin_num, pReadReq.requestedColumns, result_row);
            } else {
                _writer_manager->flush(vin_num);
                _latest_manager->query_latest<false>(vin_num, pReadReq.requestedColumns, result_row);
            }
            pReadRes.emplace_back(std::move(result_row));
        }
        return 0;
    }

    int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
        uint16_t vin_num = decode_vin(trReadReq.vin);
        if (unlikely(vin_num == INVALID_VIN_NUM)) {
            return 0;
        }
        if (_finish_compaction) {
            _tr_manager->query_time_range<true>(vin_num, trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                          trReadReq.requestedColumns, trReadRes);
        } else {
            _writer_manager->flush(vin_num);
            _tr_manager->query_time_range<false>(vin_num, trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                                          trReadReq.requestedColumns, trReadRes);
        }
        return 0;
    }

    int TSDBEngineImpl::executeAggregateQuery(const TimeRangeAggregationRequest &aggregationReq, std::vector<Row> &aggregationRes) {
        uint16_t vin_num = decode_vin(aggregationReq.vin);
        if (unlikely(vin_num == INVALID_VIN_NUM)) {
            return 0;
        }
        if (_finish_compaction) {
            _agg_manager->query_aggregate<true>(vin_num, aggregationReq.vin, aggregationReq.timeLowerBound, aggregationReq.timeUpperBound,
                                          aggregationReq.columnName, aggregationReq.aggregator, aggregationRes);
        } else {
            _writer_manager->flush(vin_num);
            _agg_manager->query_aggregate<false>(vin_num, aggregationReq.vin, aggregationReq.timeLowerBound, aggregationReq.timeUpperBound,
                                                                          aggregationReq.columnName, aggregationReq.aggregator, aggregationRes);
        }
        return 0;
    }

    int TSDBEngineImpl::executeDownsampleQuery(const TimeRangeDownsampleRequest &downsampleReq, std::vector<Row> &downsampleRes) {
        uint16_t vin_num = decode_vin(downsampleReq.vin);
        if (unlikely(vin_num == INVALID_VIN_NUM)) {
            return 0;
        }
        if (_finish_compaction) {
            _ds_manager->query_down_sample<true>(vin_num, downsampleReq.vin, downsampleReq.timeLowerBound, downsampleReq.timeUpperBound,
                                           downsampleReq.interval, downsampleReq.columnName, downsampleReq.aggregator,
                                           downsampleReq.columnFilter, downsampleRes);
        } else {
            _writer_manager->flush(vin_num);
            _ds_manager->query_down_sample<false>(vin_num, downsampleReq.vin, downsampleReq.timeLowerBound, downsampleReq.timeUpperBound,
                                                 downsampleReq.interval, downsampleReq.columnName, downsampleReq.aggregator,
                                                 downsampleReq.columnFilter, downsampleRes);
        }

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
        if (!std::filesystem::exists(_get_schema_path())) {
            return;
        }
        std::ifstream schema_fin;
        schema_fin.open(_get_schema_path(), std::ios::in);
        if (!schema_fin.is_open() || !schema_fin.good()) {
            schema_fin.close();
            return;
        }
        std::map<std::string, ColumnType> column_type_map;

        for (uint16_t i = 0; i < SCHEMA_COLUMN_NUMS; ++i) {
            std::string column_name;
            uint8_t column_type_int;
            schema_fin >> column_name;
            schema_fin >> column_type_int;
            column_type_map.emplace(column_name, (ColumnType) column_type_int);
        }

        _schema = std::make_shared<Schema>(std::move(column_type_map));
    }

    void TSDBEngineImpl::_get_latest_records() {
        std::set<std::string> column_names;

        for (const auto &item: _schema->columnTypeMap) {
            column_names.insert(item.first);
        }

#pragma omp parallel for num_threads(8)
        for (uint16_t i = 0; i < VIN_NUM_RANGE; ++i) {
            std::vector<Row> latest_row;
            _tr_manager->query_time_range<true>(i, encode_vin(i), MAX_TS, MAX_TS + 1, column_names, latest_row);
            assert(latest_row.size() == 1);
            _latest_manager->set_latest_row(i, latest_row[0]);
        }
    }

    void TSDBEngineImpl::_print_schema() {
        std::stringstream ss;

        for (const auto& pair : _schema->columnTypeMap) {
            switch (pair.second) {
                case COLUMN_TYPE_INTEGER:
                    ss << pair.first << ": { type: int }" << std::endl;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    ss << pair.first << ": { type: double }" << std::endl;
                    break;
                case COLUMN_TYPE_STRING:
                    ss << pair.first << ": { type: string }" << std::endl;
                    break;
                default:
                    break;
            }
        }

        INFO_LOG("schema:\n%s", ss.str().c_str())
    }
}