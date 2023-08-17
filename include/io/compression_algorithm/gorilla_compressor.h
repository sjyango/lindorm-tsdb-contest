#pragma once

#include <common/unaligned.h>
#include <io/BitHelpers.h>
#include <bitset>
#include <cstring>
#include <algorithm>
#include <type_traits>
#include <cassert>
#include <stdexcept>
#include "io/compression.h"

namespace LindormContest::io {
    /** Gorilla column codec implementation.
 *
 * Based on Gorilla paper: https://dl.acm.org/doi/10.14778/2824032.2824078
 *
 * This codec is best used against monotonic floating sequences, like CPU usage percentage
 * or any other gauge.
 *
 * Given input sequence a: [a0, a1, ... an]
 *
 * First, write number of items (sizeof(int32)*8 bits):                n
 * Then write first item as is (sizeof(a[0])*8 bits):                  a[0]
 * Loop over remaining items and calculate xor_diff:
 *   xor_diff = a[i] ^ a[i - 1] (e.g. 00000011'10110100)
 *   Write it in compact binary form with `BitWriter`
 *   if xor_diff == 0:
 *       write 1 bit:                                                  0
 *   else:
 *       calculate leading zero bits (lzb)
 *       and trailing zero bits (tzb) of xor_diff,
 *       compare to lzb and tzb of previous xor_diff
 *       (X = sizeof(a[i]) * 8, e.g. X = 16, lzb = 6, tzb = 2)
 *       if lzb >= prev_lzb && tzb >= prev_tzb:
 *           (e.g. prev_lzb=4, prev_tzb=1)
 *           write 2 bit prefix:                                       0b10
 *           write xor_diff >> prev_tzb (X - prev_lzb - prev_tzb bits):0b00111011010
 *           (where X = sizeof(a[i]) * 8, e.g. 16)
 *       else:
 *           write 2 bit prefix:                                       0b11
 *           write 5 bits of lzb:                                      0b00110
 *           write 6 bits of (X - lzb - tzb)=(16-6-2)=8:               0b001000
 *           write (X - lzb - tzb) non-zero bits of xor_diff:          0b11101101
 *           prev_lzb = lzb
 *           prev_tzb = tzb
 *
 * @example sequence of Float32 values [0.1, 0.1, 0.11, 0.2, 0.1] is encoded as:
 *
 * .- 4-byte little endian sequence length: 5                                 : 0x00000005
 * |                .- 4 byte (sizeof(Float32) a[0] as uint32_t : -10           : 0xcdcccc3d
 * |                |               .- 4 encoded xor diffs (see below)
 * v_______________ v______________ v__________________________________________________
 * \x05\x00\x00\x00\xcd\xcc\xcc\x3d\x6a\x5a\xd8\xb6\x3c\xcd\x75\xb1\x6c\x77\x00\x00\x00
 *
 * 4 binary encoded xor diffs (\x6a\x5a\xd8\xb6\x3c\xcd\x75\xb1\x6c\x77\x00\x00\x00):
 *
 * ...........................................
 * a[i-1]   = 00111101110011001100110011001101
 * a[i]     = 00111101110011001100110011001101
 * xor_diff = 00000000000000000000000000000000
 * .- 1-bit prefix                                                           : 0b0
 * |
 * | ...........................................
 * | a[i-1]   = 00111101110011001100110011001101
 * ! a[i]     = 00111101111000010100011110101110
 * | xor_diff = 00000000001011011000101101100011
 * | lzb = 10
 * | tzb = 0
 * |.- 2-bit prefix                                                          : 0b11
 * || .- lzb (10)                                                            : 0b1010
 * || |     .- data length (32-10-0): 22                                     : 0b010110
 * || |     |     .- data                                                    : 0b1011011000101101100011
 * || |     |     |
 * || |     |     |                        ...........................................
 * || |     |     |                        a[i-1]   = 00111101111000010100011110101110
 * || |     |     |                        a[i]     = 00111110010011001100110011001101
 * || |     |     |                        xor_diff = 00000011101011011000101101100011
 * || |     |     |                        .- 2-bit prefix                            : 0b11
 * || |     |     |                        | .- lzb = 6                               : 0b00110
 * || |     |     |                        | |     .- data length = (32 - 6) = 26     : 0b011010
 * || |     |     |                        | |     |      .- data                     : 0b11101011011000101101100011
 * || |     |     |                        | |     |      |
 * || |     |     |                        | |     |      |                            ...........................................
 * || |     |     |                        | |     |      |                            a[i-1]   = 00111110010011001100110011001101
 * || |     |     |                        | |     |      |                            a[i]     = 00111101110011001100110011001101
 * || |     |     |                        | |     |      |                            xor_diff = 00000011100000000000000000000000
 * || |     |     |                        | |     |      |                            .- 2-bit prefix                            : 0b10
 * || |     |     |                        | |     |      |                            | .- data                                  : 0b11100000000000000000000000
 * VV_v____ v_____v________________________V_v_____v______v____________________________V_v_____________________________
 * 01101010 01011010 11011000 10110110 00111100 11001101 01110101 10110001 01101100 01110111 00000000 00000000 00000000
 *
 * Please also see unit tests for:
 *   * Examples on what output `BitWriter` produces on predefined input.
 *   * Compatibility tests solidifying encoded binary output on set of predefined sequences.
 */
class CompressionCodecGorilla : public CompressionUtil {
    public:
        explicit CompressionCodecGorilla(uint8_t data_bytes_size_);
        virtual void init() override{
            //do nothing
        }
        uint32_t compress(const Slice& input, std::string* output) override;
        virtual uint32_t compress(const Slice& input, size_t uncompressed_size, std::string* output) override{
            //do nothing
        }
        uint8_t decompress(const Slice& input, Slice* output) override;
        virtual ~CompressionCodecGorilla() = default;
        
        uint32_t doCompressData(const char * source, uint32_t source_size, char * dest) const ;
        uint8_t doDecompressData(const char * source, uint32_t source_size, char * dest, uint32_t uncompressed_size) const ;
        uint32_t getMaxCompressedDataSize(uint32_t uncompressed_size) const ;
        
        inline bool isCompression() const  { return true; }
        inline bool isGenericCompression() const  { return false; }
        inline bool isFloatingPointTimeSeriesCodec() const  { return true; }
    private:
        const uint8_t data_bytes_size;
    };
    namespace ErrorCodes
    {
        extern const int CANNOT_COMPRESS;
        extern const int CANNOT_DECOMPRESS;
        extern const int BAD_ARGUMENTS;
        extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
        extern const int ILLEGAL_CODEC_PARAMETER;
    }


    //    void registerCodecGorilla(CompressionCodecFactory & factory)
    //    {
    //        uint8_t method_code = static_cast<uint8_t>(CompressionMethodByte::Gorilla);
    //        auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    //        {
    //            /// Default bytes size is 1
    //            uint8_t data_bytes_size = 1;
    //            if (arguments && !arguments->children.empty())
    //            {
    //                if (arguments->children.size() > 1)
    //                    throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "Gorilla codec must have 1 parameter, given {}", arguments->children.size());
    //                const auto children = arguments->children;
    //                const auto * literal = children[0]->as<ASTLiteral>();
    //                if (!literal || literal->value.getType() != Field::Types::Which::uint64_t)
    //                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Gorilla codec argument must be unsigned integer");
    //                size_t user_bytes_size = literal->value.safeGet<uint64_t>();
    //                if (user_bytes_size != 1 && user_bytes_size != 2 && user_bytes_size != 4 && user_bytes_size != 8)
    //                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Argument value for Gorilla codec can be 1, 2, 4 or 8, given {}", user_bytes_size);
    //                data_bytes_size = static_cast<uint8_t>(user_bytes_size);
    //            }
    //            else if (column_type)
    //            {
    //                data_bytes_size = getDataBytesSize(column_type);
    //            }
    //            return std::make_shared<CompressionCodecGorilla>(data_bytes_size);
    //        };
    //        factory.registerCompressionCodecWithType("Gorilla", method_code, codec_builder);
    //    }
}
