#include "compression/float_compression.h"

namespace LindormContest::io{

namespace {
constexpr uint8_t getBitLengthOfLength(uint8_t data_bytes_size)
{
  // 1-byte value is 8 bits, and we need 4 bits to represent 8 : 1000,
  // 2-byte         16 bits        =>    5
  // 4-byte         32 bits        =>    6
  // 8-byte         64 bits        =>    7
  const uint8_t bit_lengths[] = {0, 4, 5, 0, 6, 0, 0, 0, 7};
  assert(data_bytes_size >= 1 && data_bytes_size < sizeof(bit_lengths) && bit_lengths[data_bytes_size] != 0);
  return bit_lengths[data_bytes_size];
}
uint32_t getCompressedHeaderSize(uint8_t data_bytes_size)
{
  constexpr uint8_t items_count_size = 4;
  return items_count_size + data_bytes_size;
}
uint32_t getCompressedDataSize(uint8_t data_bytes_size, uint32_t uncompressed_size)
{
  const uint32_t items_count = uncompressed_size / data_bytes_size;
  static const auto DATA_BIT_LENGTH = getBitLengthOfLength(data_bytes_size);
  // -1 since there must be at least 1 non-zero bit.
  static const auto LEADING_ZEROES_BIT_LENGTH = DATA_BIT_LENGTH - 1;
  // worst case (for 32-bit value):
  // 11 + 5 bits of leading zeroes bit-size + 5 bits of data bit-size + non-zero data bits.
  const uint32_t max_item_size_bits = 2 + LEADING_ZEROES_BIT_LENGTH + DATA_BIT_LENGTH + data_bytes_size * 8;
  // + 8 is to round up to next byte.
  return (items_count * max_item_size_bits + 8) / 8;
}
struct BinaryValueInfo
{
  uint8_t leading_zero_bits;
  uint8_t data_bits;
  uint8_t trailing_zero_bits;
};
template <typename T>
BinaryValueInfo getBinaryValueInfo(const T & value)
{
  constexpr uint8_t bit_size = sizeof(T) * 8;
  const uint8_t lz = getLeadingZeroBits(value);
  const uint8_t tz = getTrailingZeroBits(value);
  const uint8_t data_size = value == 0 ? 0 : static_cast<uint8_t>(bit_size - lz - tz);
  return {lz, data_size, tz};
}
template <typename T>
uint32_t compressDataForType(const char * source, uint32_t source_size, char * dest, uint32_t dest_size)
{
  if (source_size % sizeof(T) != 0)
    throw std::runtime_error("Cannot compress, data size {} is not aligned to {}");
  const char * const source_end = source + source_size;
  const char * const dest_start = dest;
  const char * const dest_end = dest + dest_size;
  const uint32_t items_count = source_size / sizeof(T);
  unalignedStoreLittleEndian<uint32_t>(dest, items_count);
  dest += sizeof(items_count);
  T prev_value = 0;
  // That would cause first XORed value to be written in-full.
  BinaryValueInfo prev_xored_info{0, 0, 0};
  if (source < source_end)
  {
    prev_value = unalignedLoadLittleEndian<T>(source);
    unalignedStoreLittleEndian<T>(dest, prev_value);
    source += sizeof(prev_value);
    dest += sizeof(prev_value);
  }
  BitWriter writer(dest, dest_end - dest);
  static const auto DATA_BIT_LENGTH = getBitLengthOfLength(sizeof(T));
  // -1 since there must be at least 1 non-zero bit.
  static const auto LEADING_ZEROES_BIT_LENGTH = DATA_BIT_LENGTH - 1;
  while (source < source_end)
  {
    const T curr_value = unalignedLoadLittleEndian<T>(source);
    source += sizeof(curr_value);
    const auto xored_data = curr_value ^ prev_value;
    const BinaryValueInfo curr_xored_info = getBinaryValueInfo(xored_data);
    if (xored_data == 0)
    {
      writer.writeBits(1, 0);
    }
    else if (prev_xored_info.data_bits != 0
             && prev_xored_info.leading_zero_bits <= curr_xored_info.leading_zero_bits
             && prev_xored_info.trailing_zero_bits <= curr_xored_info.trailing_zero_bits)
    {
      writer.writeBits(2, 0b10);
      writer.writeBits(prev_xored_info.data_bits, xored_data >> prev_xored_info.trailing_zero_bits);
    }
    else
    {
      writer.writeBits(2, 0b11);
      writer.writeBits(LEADING_ZEROES_BIT_LENGTH, curr_xored_info.leading_zero_bits);
      writer.writeBits(DATA_BIT_LENGTH, curr_xored_info.data_bits);
      writer.writeBits(curr_xored_info.data_bits, xored_data >> curr_xored_info.trailing_zero_bits);
      prev_xored_info = curr_xored_info;
    }
    prev_value = curr_value;
  }
  writer.flush();
  return static_cast<uint32_t>((dest - dest_start) + (writer.count() + 7) / 8);
}
template <typename T>
void decompressDataForType(const char * source, uint32_t source_size, char * dest)
{
  const char * const source_end = source + source_size;
  if (source + sizeof(uint32_t) > source_end)
    return;
  const uint32_t items_count = unalignedLoadLittleEndian<uint32_t>(source);
  source += sizeof(items_count);
  T prev_value = 0;
  // decoding first item
  if (source + sizeof(T) > source_end || items_count < 1)
    return;
  prev_value = unalignedLoadLittleEndian<T>(source);
  unalignedStoreLittleEndian<T>(dest, prev_value);
  source += sizeof(prev_value);
  dest += sizeof(prev_value);
  BitReader reader(source, source_size - sizeof(items_count) - sizeof(prev_value));
  BinaryValueInfo prev_xored_info{0, 0, 0};
  static const auto DATA_BIT_LENGTH = getBitLengthOfLength(sizeof(T));
  // -1 since there must be at least 1 non-zero bit.
  static const auto LEADING_ZEROES_BIT_LENGTH = DATA_BIT_LENGTH - 1;
  // since data is tightly packed, up to 1 bit per value, and last byte is padded with zeroes,
  // we have to keep track of items to avoid reading more that there is.
  for (uint32_t items_read = 1; items_read < items_count && !reader.eof(); ++items_read)
  {
    T curr_value = prev_value;
    BinaryValueInfo curr_xored_info = prev_xored_info;
    T xored_data = 0;
    if (reader.readBit() == 1)
    {
      if (reader.readBit() == 1)
      {
        // 0b11 prefix
        curr_xored_info.leading_zero_bits = reader.readBits(LEADING_ZEROES_BIT_LENGTH);
        curr_xored_info.data_bits = reader.readBits(DATA_BIT_LENGTH);
        curr_xored_info.trailing_zero_bits = sizeof(T) * 8 - curr_xored_info.leading_zero_bits - curr_xored_info.data_bits;
      }
      // else: 0b10 prefix - use prev_xored_info
      if (curr_xored_info.leading_zero_bits == 0
          && curr_xored_info.data_bits == 0
          && curr_xored_info.trailing_zero_bits == 0) [[unlikely]]
      {
        throw std::runtime_error("Cannot decompress gorilla-encoded data: corrupted input data.");
      }
      xored_data = static_cast<T>(reader.readBits(curr_xored_info.data_bits));
      xored_data <<= curr_xored_info.trailing_zero_bits;
      curr_value = prev_value ^ xored_data;
    }
    // else: 0b0 prefix - use prev_value
    unalignedStoreLittleEndian<T>(dest, curr_value);
    dest += sizeof(curr_value);
    prev_xored_info = curr_xored_info;
    prev_value = curr_value;
  }
}
}

CompressionCodecGorilla::CompressionCodecGorilla(uint8_t data_bytes_size_)
    : data_bytes_size(data_bytes_size_){}

uint32_t CompressionCodecGorilla::getMaxCompressedDataSize(uint32_t uncompressed_size) const {
  const auto result = 2 // common header
                      + data_bytes_size // max bytes skipped if source is not properly aligned.
                      + getCompressedHeaderSize(data_bytes_size) // data-specific header
                      + getCompressedDataSize(data_bytes_size, uncompressed_size);
  return result;
}


uint32_t CompressionCodecGorilla::compress(const char * source, uint32_t source_size, char * dest) const
{
  uint8_t bytes_to_skip = source_size % data_bytes_size;
  dest[0] = data_bytes_size;
  dest[1] = bytes_to_skip; /// unused (backward compatibility)
  memcpy(&dest[2], source, bytes_to_skip);
  size_t start_pos = 2 + bytes_to_skip;
  uint32_t result_size = 0;
  const uint32_t compressed_size = getMaxCompressedDataSize(source_size);
  switch (data_bytes_size)
  {
  case 1:
    result_size = compressDataForType<uint8_t>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
    break;
  case 2:
    result_size = compressDataForType<uint16_t>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
    break;
  case 4:
    result_size = compressDataForType<uint32_t>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
    break;
  case 8:
    result_size = compressDataForType<uint64_t>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], compressed_size);
    break;
  }
  return 2 + bytes_to_skip + result_size;
}

uint8_t CompressionCodecGorilla::decompress(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const {
  if (source_size < 2)
    throw std::runtime_error("Cannot decompress. File has wrong header");
  uint8_t bytes_size = source[0];
  if (bytes_size == 0)
    throw std::runtime_error("Cannot decompress. File has wrong header");
  uint8_t bytes_to_skip = uncompressed_size % bytes_size;
  if (static_cast<uint32_t>(2 + bytes_to_skip) > source_size)
    throw std::runtime_error("Cannot decompress. File has wrong header");

  memcpy(dest, &source[2], bytes_to_skip);
  uint32_t source_size_no_header = source_size - bytes_to_skip - 2;
  switch (bytes_size)
  {
  case 1:
    decompressDataForType<uint8_t>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
    break;
  case 2:
    decompressDataForType<uint16_t>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
    break;
  case 4:
    decompressDataForType<uint32_t>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
    break;
  case 8:
    decompressDataForType<uint64_t>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip]);
    break;
  }
  return bytes_to_skip;
}

}
