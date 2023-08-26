#include <random>

#include <gtest/gtest.h>
#include "compression/compressor.h"

namespace LindormContest::compression::test {

    static std::string generate_random_string(int length) {
        const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, charset.size() - 1);

        std::string str(length, '\0');
        for (int i = 0; i < length; ++i) {
            str[i] = charset[dis(gen)];
        }

        return str;
    }

    static int32_t generate_random_int32() {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<int32_t> dis(0, std::numeric_limits<int32_t>::max());
        return dis(gen);
    }

    static double_t generate_random_float64() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<double> dis(0.0, 1.0);
        return dis(gen);
    }

    template<typename T>
    void verifyResult(const T* input, const char *result, size_t length) {
        auto *base = reinterpret_cast<T*>(const_cast<char*>(result));
        for (auto i = 0; i < length; ++i) {
            assert(input[i] == *base);
            if (i != length - 1)base++;
        }
    }

    TEST(Compression, float_test) {
        static constexpr size_t N = 100000;
        std::vector<double> input;

        for (int i = 0; i < N; ++i) {
            input.emplace_back(generate_random_float64());
        }

        auto length = input.size();
        uint32_t uncompressSize = length * sizeof(double);

        // pre-allocate a large size
        char *origin = reinterpret_cast<char *>(input.data());
        char *compress = reinterpret_cast<char *>(malloc(uncompressSize));

        uint32_t compress_size = LindormContest::compression::compressFloat(origin, uncompressSize, compress);

        char *recover = reinterpret_cast<char *>(malloc(uncompressSize));

        auto dest = LindormContest::compression::decompressFloat(compress, compress_size, recover, uncompressSize);

        verifyResult<double>(input.data(), dest, input.size());

        free(recover);
        free(compress);
        GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
        GTEST_LOG_(INFO) << "compress ratio: " << (uncompressSize - compress_size) * 1.0 / uncompressSize;
    }

    TEST(Compression, int_test) {
        static constexpr size_t N = 100000;
        std::vector<int> input;

        for (int i = 0; i < N; ++i) {
            input.emplace_back(generate_random_int32());
        }

        size_t uncompressSize = input.size() * sizeof(int);

        // pre-allocate a large size
        char *origin = reinterpret_cast<char *>(input.data());
        char *compress = reinterpret_cast<char *>(malloc(uncompressSize * 2));

        uint32_t compress_size = LindormContest::compression::compressInteger(origin, uncompressSize, compress);

        char *recover = reinterpret_cast<char *>(malloc(uncompressSize * 2));

        auto dest = LindormContest::compression::decompressInteger(compress, compress_size, recover, uncompressSize);

        verifyResult<int>(input.data(), dest, input.size());

        free(compress);
        free(recover);
        GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
        GTEST_LOG_(INFO) << "compress ratio: " << (uncompressSize - compress_size) * 1.0 / uncompressSize;
    }

    TEST(Compression, string_test) {
        static constexpr size_t N = 100000;
        std::string input = generate_random_string(N);
        size_t uncompressSize = input.size();

        // pre-allocate a large size
        char *origin = input.data();
        char *compress = reinterpret_cast<char *>(malloc(uncompressSize * 2));

        uint32_t compress_size = LindormContest::compression::compressString(origin, uncompressSize, compress);

        char *recover = reinterpret_cast<char *>(malloc(uncompressSize * 2));

        LindormContest::compression::decompressString(compress, compress_size, recover, uncompressSize);

        verifyResult<char>(input.data(), recover, input.size());
        free(recover);
        free(compress);
        GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
        GTEST_LOG_(INFO) << "compress ratio: " << (uncompressSize - compress_size) * 1.0 / uncompressSize;
    }
}
