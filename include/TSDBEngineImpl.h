//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#ifndef LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
#define LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H

#include "TSDBEngine.hpp"
#include "storage/memmap.h"

namespace LindormContest {
    const int32_t SCHEMA_COLUMN_NUMS = 60;

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

    int executeAggregateQuery(const TimeRangeAggregationRequest &aggregationReq, std::vector<Row> &aggregationRes) override;

    int executeDownsampleQuery(const TimeRangeDownsampleRequest &downsampleReq, std::vector<Row> &downsampleRes) override;

private:
    Path _get_root_path() {
        return dataDirPath;
    }

    void _save_schema_to_file();

    void _load_schema_from_file();

    ColumnType _column_types[SCHEMA_COLUMN_NUMS]{};
    std::string _column_names[SCHEMA_COLUMN_NUMS]{};
    SchemaSPtr _schema;
    storage::ShardMemMap _shard_mem_map;
}; // End class TSDBEngineImpl.

}

#endif //LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
