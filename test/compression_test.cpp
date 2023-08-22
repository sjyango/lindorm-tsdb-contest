#include <gtest/gtest.h>
#include "compression/compressor.h"

namespace LindormContest::test {

template<typename T>
void verifyResult(std::vector<T>& input, const char* result){
    auto length = input.size();
    auto* base = reinterpret_cast<T*>(const_cast<char *>(result));
    for(auto i = 0 ;i < length;++i){
      assert(input[i] == *base);
      if(i != length - 1)base++;
    }
}
TEST(Compression, float_test) {
  static constexpr size_t BIG_INT = 100;
  std::vector<double> input = {3.1,3.1,3.1,3.1,3.1};
  auto length = input.size();
  uint32_t uncompressSize = length * sizeof(double);
  
  // pre-allocate a large size
  char* origin = reinterpret_cast<char *>(input.data());
  char *compress = reinterpret_cast<char *>(malloc(uncompressSize));
    
  uint32_t compress_size = LindormContest::io::compressFloat(origin,uncompressSize,compress);
  
  char *recover = reinterpret_cast<char*>(malloc(BIG_INT));

  auto skip = LindormContest::io::decompressFloat(compress,compress_size,recover,uncompressSize);
    
  verifyResult<double>(input,recover + skip);
  
  free(recover);
  free(compress);
  GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
  GTEST_LOG_(INFO) << "compress ratio: " << (uncompressSize - compress_size) * 1.0 / uncompressSize;
}
    
TEST(Compression, int_test) {
  static constexpr size_t BIG_INT = 100;
  std::vector<int> input = {3,3,3,3,3,3,3,3};
  auto length = input.size();
  uint32_t uncompressSize = length * sizeof(int);
  
  // pre-allocate a large size
  char* origin = reinterpret_cast<char *>(input.data());
  char *compress = reinterpret_cast<char *>(malloc(uncompressSize));
    
  uint32_t compress_size = LindormContest::io::compressInteger(origin,uncompressSize,compress);
  
  char *recover = reinterpret_cast<char*>(malloc(BIG_INT));
  
  auto skip = LindormContest::io::decompressInteger(compress,compress_size,recover,uncompressSize);
    
  verifyResult<int>(input,recover + skip);
  
  free(recover);
  free(compress);
  GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
  GTEST_LOG_(INFO) << "compress ratio: " << (uncompressSize - compress_size) * 1.0 / uncompressSize;
}

TEST(Compression, string_test) {
  static constexpr size_t BIG_INT = 1000;
//  std::vector<char> input = {'a','a','c','a','a','c','a','a',
//                              'a','h','s','d','k','l','c','a','d'};
  std::vector<char> input;
  input.reserve(100);
  for(int i = 0; i< 100; i++){
      input.emplace_back('a');
  }
  auto length = input.size();
  uint32_t uncompressSize = length * sizeof(char);
  
  // pre-allocate a large size
  char* origin = reinterpret_cast<char *>(input.data());
  char *compress = reinterpret_cast<char *>(malloc(uncompressSize));
    
  uint32_t compress_size = LindormContest::io::compressString(origin,uncompressSize,compress);
  
  char *recover = reinterpret_cast<char*>(malloc(BIG_INT));
  
  LindormContest::io::decompressString(compress,compress_size,recover,uncompressSize);

  verifyResult<char>(input,recover);
  free(recover);
  free(compress);
  GTEST_LOG_(INFO) << "original size: " << uncompressSize << "; compress size: " << compress_size;
  GTEST_LOG_(INFO) << "compress ratio: " << (uncompressSize - compress_size) * 1.0 / uncompressSize;
}
}
