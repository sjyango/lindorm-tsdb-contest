//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#ifndef LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
#define LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H

#include "Root.h"
#include "TSDBEngine.hpp"
#include "latest_manager.h"
#include "storage/memmap.h"

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

        Path _get_latest_records_path() const { return _get_root_path() / "latest_records.dat"; }

        void _save_schema_to_file();

        void _load_schema_from_file();

        SchemaSPtr _schema;
        LatestManager _latest_manager;
        ShardMemMap _shard_mem_map;
    }; // End class TSDBEngineImpl.

}

#endif //LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
