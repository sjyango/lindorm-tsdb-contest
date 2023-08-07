#include <algorithm>
#include <filesystem>
#include "TSDBEngineImpl.h"

static LindormContest::Vin vin1;
static LindormContest::Vin vin2;
static LindormContest::Row row1;
static LindormContest::Row row2;
static LindormContest::Row row3;
static char str1[20];
static char str2[19];

static int createTable(LindormContest::TSDBEngine *engine) {
    LindormContest::Schema schema1;
    schema1.columnTypeMap["t1c1"] = LindormContest::COLUMN_TYPE_INTEGER;
    schema1.columnTypeMap["t1c2"] = LindormContest::COLUMN_TYPE_DOUBLE_FLOAT;
    schema1.columnTypeMap["t1c3"] = LindormContest::COLUMN_TYPE_STRING;
    int ret = engine->createTable("t1", schema1);
    if (ret != 0) {
        std::cerr << "Create table 1 failed" << std::endl;
        return -1;
    }
    return 0;
}

static void prepareStaticVariables() {
    // Create 2 vins.
    for (int i = 0; i < 17; ++i) {
        vin1.vin[i] = 'a' + i;
    }
    for (int i = 0; i < 17; ++i) {
        vin2.vin[i] = 'b' + i;
    }

    // Create 2 binary string.
    int64_t tmpI64;
    tmpI64 = -2354;
    memset(str1, '1', 20);
    memcpy(str1, &tmpI64, sizeof(int64_t));
    memcpy(str1 + 8, &tmpI64, sizeof(int64_t));
    memset(str2, '1', 19);
    memcpy(str2, &tmpI64, sizeof(int64_t));
    memcpy(str2 + 9, &tmpI64, sizeof(int64_t));

    // Row 1.
    row1.vin = vin1;
    row1.timestamp = 1;
    row1.columns.insert(std::make_pair("t1c1", 100));
    row1.columns.insert(std::make_pair("t1c2", 100.1));
    row1.columns.insert(std::make_pair("t1c3", LindormContest::ColumnValue(str1, 20)));

    // Row 2.
    row2.vin = vin2;
    row2.timestamp = 3;
    row2.columns.insert(std::make_pair("t1c1", 101));
    row2.columns.insert(std::make_pair("t1c2", 101.1));
    row2.columns.insert(std::make_pair("t1c3", LindormContest::ColumnValue(str1, 20)));

    // Row 3.
    row3.vin = vin1;
    row3.timestamp = 2;
    row3.columns.insert(std::make_pair("t1c1", 102));
    row3.columns.insert(std::make_pair("t1c2", 102.1));
    row3.columns.insert(std::make_pair("t1c3", LindormContest::ColumnValue(str2, 19)));
}

static int verifyTableData(LindormContest::TSDBEngine *engine) {
    // Execute latest query for part.
    LindormContest::LatestQueryRequest pReadReq;
    std::vector<LindormContest::Row> pReadRes;
    pReadReq.tableName = "t1";
    pReadReq.requestedColumns.insert("t1c1");
    pReadReq.vins.push_back(vin1);
    int ret = engine->executeLatestQuery(pReadReq, pReadRes);
    if (ret != 0) {
        std::cerr << "Cannot query" << std::endl;
        return -1;
    }

    // Verify the query result.
    if (pReadRes.size() != 1) {
        std::cerr << "Latest res number is not correct" << std::endl;
        return -1;
    }
    if (pReadRes.begin()->vin != vin1 || pReadRes.begin()->timestamp != 2) {
        std::cerr << "Latest res content is not correct" << std::endl;
        return -1;
    }
    if (pReadRes.begin()->columns.size() != 1) {
        std::cerr << "Latest res's column number is not correct" << std::endl;
        return -1;
    }
    int32_t t1c1Val;
    ret = pReadRes.begin()->columns.begin()->second.getIntegerValue(t1c1Val);
    if (ret != 0 || t1c1Val != 102) {
        std::cerr << "Latest res content is not correct" << std::endl;
        return -1;
    }

    // Execute latest query for full.
    pReadReq.requestedColumns.insert("t1c2");
    pReadReq.requestedColumns.insert("t1c3");
    pReadReq.vins.push_back(vin2);
    pReadRes.clear();
    ret = engine->executeLatestQuery(pReadReq, pReadRes);
    if (ret != 0) {
        std::cerr << "Cannot query" << std::endl;
        return -1;
    }
    if (pReadRes.size() != 2) {
        std::cerr << "Latest res number is not correct" << std::endl;
        return -1;
    }
    std::sort(pReadRes.begin(), pReadRes.end());
    LindormContest::Row &r0 = pReadRes[0];
    LindormContest::Row &r1 = pReadRes[1];
    int32_t intBuff;
    double_t doubleBuff;
    std::pair<int32_t, const char *> strBuff;
    if (r0.vin != vin1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.timestamp != 2 || r0.columns.size() != 3) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.columns["t1c1"].getIntegerValue(intBuff) != 0 || intBuff != 102) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.columns["t1c2"].getDoubleFloatValue(doubleBuff) != 0 || doubleBuff != 102.1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.columns["t1c3"].getStringValue(strBuff) != 0
        || strBuff.first != 19
        || std::strncmp(strBuff.second, str2, 19) != 0) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.vin != vin2) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.timestamp != 3 || r1.columns.size() != 3) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.columns["t1c1"].getIntegerValue(intBuff) != 0 || intBuff != 101) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.columns["t1c2"].getDoubleFloatValue(doubleBuff) != 0 || doubleBuff != 101.1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    strBuff.second = nullptr;
    if (r1.columns["t1c3"].getStringValue(strBuff) != 0
        || strBuff.first != 20
        || std::strncmp(strBuff.second, str1, 20) != 0) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }

    // Execute time range query for part.
    LindormContest::TimeRangeQueryRequest trR;
    trR.vin = vin1;
    trR.tableName = "t1";
    trR.timeLowerBound = 1;
    trR.timeUpperBound = 2;
    trR.requestedColumns.insert("t1c1");
    std::vector<LindormContest::Row> trReadRes;
    ret = engine->executeTimeRangeQuery(trR, trReadRes);
    if (ret != 0) {
        std::cerr << "Query time range failed" << std::endl;
        return -1;
    }
    if (trReadRes.size() != 1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (trReadRes[0].vin != vin1 || trReadRes[0].timestamp != 1
        || trReadRes[0].columns.size() != 1
        || trReadRes[0].columns.begin()->second.getIntegerValue(intBuff) != 0) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (intBuff != 100) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }

    // Execute time range query for full.
    trR.timeLowerBound = 1;
    trR.timeUpperBound = 6;
    trR.requestedColumns.clear();
    trReadRes.clear();
    ret = engine->executeTimeRangeQuery(trR, trReadRes);
    if (ret != 0) {
        std::cerr << "Query time range failed" << std::endl;
        return -1;
    }
    if (trReadRes.size() != 2) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }

    return 0;
}

static int writeDataTo(LindormContest::TSDBEngine *engine) {
    // Execute upsert.
    LindormContest::WriteRequest wReq;
    wReq.tableName = "t1";
    wReq.rows.push_back(row1);
    wReq.rows.push_back(row2);
    int ret = engine->upsert(wReq);
    if (ret != 0) {
        std::cerr << "Upsert failed" << std::endl;
        return ret;
    }

    wReq.rows.clear();
    wReq.rows.push_back(row3);
    ret = engine->upsert(wReq);
    if (ret != 0) {
        std::cerr << "Upsert failed" << std::endl;
        return ret;
    }

    return 0;
}

int main() {

    std::filesystem::path dbPath = std::filesystem::path("/tmp/db_tsdb_test");
    std::filesystem::remove_all(dbPath);
    std::filesystem::create_directory(dbPath);

    prepareStaticVariables();

    LindormContest::TSDBEngine *engine = new LindormContest::TSDBEngineImpl(dbPath.string());
    int ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect db failed" << std::endl;
        return -1;
    }
    ret = createTable(engine);
    if (ret != 0) {
        std::cerr << "Create table failed" << std::endl;
        return -1;
    }
    ret = writeDataTo(engine);
    if (ret != 0) {
        std::cerr << "Write data failed" << std::endl;
        return -1;
    }
    ret = verifyTableData(engine);
    if (ret != 0) {
        std::cerr << "Verify data before we restart the db failed" << std::endl;
        return -1;
    }
    std::cout << "PASSED data verification before we restart the db" << std::endl;

    // Restart db.
    engine->shutdown();
    delete engine;
    engine = new LindormContest::TSDBEngineImpl(dbPath.string());
    ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect db failed" << std::endl;
        return -1;
    }

    // Verify data.
    ret = verifyTableData(engine);
    if (ret != 0) {
        std::cerr << "Verify data after we restart the db failed" << std::endl;
        return -1;
    }
    std::cout << "PASSED data verification after we restart the db" << std::endl;

    engine->shutdown();
    delete engine;
    return 0;
}