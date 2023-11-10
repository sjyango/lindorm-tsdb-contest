//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#pragma once

#include "base.h"
#include "TSDBEngine.hpp"
#include "latest_manager.h"
#include "index_manager.h"
#include "time_range_manager.h"
#include "aggregate_manager.h"
#include "downsample_manager.h"
#include "convert_manager.h"
#include "storage/tsm_writer.h"

namespace LindormContest {
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

        int write(const WriteRequest &writeRequest) override;

        int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override;

        int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override;

        int executeAggregateQuery(const TimeRangeAggregationRequest &aggregationReq,
                                  std::vector<Row> &aggregationRes) override;

        int executeDownsampleQuery(const TimeRangeDownsampleRequest &downsampleReq,
                                   std::vector<Row> &downsampleRes) override;

    private:
        Path _get_root_path() const { return dataDirPath; }

        Path _get_schema_path() const { return _get_root_path() / "schema.txt"; }

        void _get_latest_records();

        void _save_schema_to_file();

        void _load_schema_from_file();

        void _print_schema();

        bool _finish_compaction;
        SchemaSPtr _schema;
        TsmWriterManagerUPtr _writer_manager;
        GlobalIndexManagerSPtr _index_manager;
        GlobalLatestManagerUPtr _latest_manager;
        GlobalTimeRangeManagerUPtr _tr_manager;
        GlobalAggregateManagerUPtr _agg_manager;
        GlobalDownSampleManagerUPtr _ds_manager;
        GlobalConvertManagerSPtr _convert_manager;
    }; // End class TSDBEngineImpl.

}

