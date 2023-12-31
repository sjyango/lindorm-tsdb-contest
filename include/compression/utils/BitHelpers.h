#pragma once

#include <cstdint>
#include <cstring>
#include <cassert>
#include <algorithm>
#include <stdexcept>

namespace LindormContest::compression {

    template<typename T>
    inline uint32_t getLeadingZeroBitsUnsafe(T x) {
        assert(x != 0);

        if constexpr (sizeof(T) <= sizeof(unsigned int)) {
            return __builtin_clz(x);
        } else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
        {
            return __builtin_clzl(x);
        } else {
            return __builtin_clzll(x);
        }
    }


    template<typename T>
    inline size_t getLeadingZeroBits(T x) {
        if (!x)
            return sizeof(x) * 8;

        return getLeadingZeroBitsUnsafe(x);
    }

// Unsafe since __builtin_ctz()-family explicitly state that result is undefined on x == 0
    template<typename T>
    inline size_t getTrailingZeroBitsUnsafe(T x) {
        assert(x != 0);

        if constexpr (sizeof(T) <= sizeof(unsigned int)) {
            return __builtin_ctz(x);
        } else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
        {
            return __builtin_ctzl(x);
        } else {
            return __builtin_ctzll(x);
        }
    }

    template<typename T>
    inline size_t getTrailingZeroBits(T x) {
        if (!x)
            return sizeof(x) * 8;

        return getTrailingZeroBitsUnsafe(x);
    }

/** Returns a mask that has '1' for `bits` LSB set:
  * maskLowBits<UInt8>(3) => 00000111
  * maskLowBits<Int8>(3) => 00000111
  */
    template<typename T>
    inline T maskLowBits(unsigned char bits) {
        using UnsignedT = std::make_unsigned_t<T>;
        if (bits == 0) {
            return 0;
        }

        UnsignedT result = static_cast<UnsignedT>(~UnsignedT{0});
        if (bits < sizeof(T) * 8) {
            result = static_cast<UnsignedT>(result >> (sizeof(UnsignedT) * 8 - bits));
        }

        return static_cast<T>(result);
    }


    template<typename T>
    constexpr T byteswap(T value) noexcept {
//        static_assert(std::is_integral_v<T>, "T must be an integral type.");
//        static_assert(std::is_pod_v<T>, "T must be a POD type.");

        T result = 0;
        const std::size_t size = sizeof(T);
        const std::size_t byteSize = 8; // 8 bits per byte

        for (std::size_t i = 0; i < size; ++i) {
            auto byte = static_cast<unsigned char>((value >> (i * byteSize)) & 0xFF);
            result |= static_cast<T>(byte) << ((size - 1 - i) * byteSize);
        }
        return result;
    }


/** Reads data from underlying ReadBuffer bit by bit, max 64 bits at once.
 *
 * reads MSB bits first, imagine that you have a data:
 * 11110000 10101010 00100100 11111110
 *
 * Given that r is BitReader created with a ReadBuffer that reads from data above:
 *  r.readBits(3)  => 0b111
 *  r.readBit()    => 0b1
 *  r.readBits(8)  => 0b1010 // 4 leading zero-bits are not shown
 *  r.readBit()    => 0b1
 *  r.readBit()    => 0b0
 *  r.readBits(15) => 0b10001001001111111
 *  r.readBit()    => 0b0
**/

    class BitReader {
        const char *const source_begin;
        const char *const source_end;
        const char *source_current;

        using BufferType = unsigned __int128;
        BufferType bits_buffer = 0;

        uint8_t bits_count = 0;

    public:
        BitReader(const char *begin, size_t size)
                : source_begin(begin), source_end(begin + size), source_current(begin) {}

        ~BitReader() = default;

        // reads bits_to_read high-bits from bits_buffer
        uint64_t readBits(uint8_t bits_to_read) {
            if (bits_to_read > bits_count)
                fillBitBuffer();

            return getBitsFromBitBuffer<CONSUME>(bits_to_read);
        }

        uint8_t peekByte() {
            if (bits_count < 8)
                fillBitBuffer();

            return getBitsFromBitBuffer<PEEK>(8);
        }

        uint8_t readBit() {
            return static_cast<uint8_t>(readBits(1));
        }

        // skip bits from bits_buffer
        void skipBufferedBits(uint8_t bits) {
            bits_buffer <<= bits;
            bits_count -= bits;
        }


        bool eof() const {
            return bits_count == 0 && source_current >= source_end;
        }

        // number of bits that was already read by clients with readBits()
        uint64_t count() const {
            return (source_current - source_begin) * 8 - bits_count;
        }

        uint64_t remaining() const {
            return (source_end - source_current) * 8 + bits_count;
        }

    private:
        enum GetBitsMode {
            CONSUME, PEEK
        };

        // read data from internal buffer, if it has not enough bits, result is undefined.
        template<GetBitsMode mode>
        uint64_t getBitsFromBitBuffer(uint8_t bits_to_read) {
            assert(bits_to_read > 0);

            // push down the high-bits
            const uint64_t result = static_cast<uint64_t>(bits_buffer >> (sizeof(bits_buffer) * 8 - bits_to_read));

            if constexpr (mode == CONSUME) {
                // 'erase' high-bits that were have read
                skipBufferedBits(bits_to_read);
            }

            return result;
        }


        // Fills internal bits_buffer with data from source, reads at most 64 bits
        inline size_t fillBitBuffer() {
            const size_t available = source_end - source_current;
            const auto bytes_to_read = std::min<size_t>(64 / 8, available);
            if (available == 0) {
                if (bytes_to_read == 0)
                    return 0;

                throw std::runtime_error("Buffer is empty, but requested to read {} more bytes.");
            }

            uint64_t tmp_buffer = 0;
            memcpy(&tmp_buffer, source_current, bytes_to_read);
            source_current += bytes_to_read;

            tmp_buffer = byteswap(tmp_buffer);

            bits_buffer |= BufferType(tmp_buffer) << ((sizeof(BufferType) - sizeof(tmp_buffer)) * 8 - bits_count);
            bits_count += static_cast<uint8_t>(bytes_to_read) * 8;

            return bytes_to_read;
        }
    };

    class BitWriter {
        char *dest_begin;
        char *dest_end;
        char *dest_current;

        using BufferType = unsigned __int128;
        BufferType bits_buffer = 0;

        uint8_t bits_count = 0;

        static constexpr uint8_t BIT_BUFFER_SIZE = sizeof(bits_buffer) * 8;

    public:
        BitWriter(char *begin, size_t size)
                : dest_begin(begin), dest_end(begin + size), dest_current(begin) {}

        ~BitWriter() {
            flush();
        }

        // write `bits_to_write` low-bits of `value` to the buffer
        void writeBits(uint8_t bits_to_write, uint64_t value) {
            assert(bits_to_write > 0);

            uint32_t capacity = BIT_BUFFER_SIZE - bits_count;
            if (capacity < bits_to_write) {
                doFlush();
                capacity = BIT_BUFFER_SIZE - bits_count;
            }

            // write low bits of value as high bits of bits_buffer
            const uint64_t mask = maskLowBits<uint64_t>(bits_to_write);
            BufferType v = value & mask;
            v <<= capacity - bits_to_write;

            bits_buffer |= v;
            bits_count += bits_to_write;
        }

        // flush contents of bits_buffer to the dest_current, partial bytes are completed with zeroes.
        void flush() {
            bits_count =
                    (bits_count + 8 - 1) &
                    ~(8 - 1); // align up to 8-bytes, so doFlush will write all data from bits_buffer
            while (bits_count != 0)
                doFlush();
        }

        uint64_t count() const {
            return (dest_current - dest_begin) * 8 + bits_count;
        }

    private:
        void doFlush() {
            // write whole bytes to the dest_current, leaving partial bits in bits_buffer
            const size_t available = dest_end - dest_current;
            const size_t to_write = std::min<size_t>(sizeof(uint64_t), bits_count / 8); // align to 8-bit boundary

            if (available < to_write) {
                throw std::runtime_error(
                        "Can not write past end of buffer. Space available is {} bytes, required to write {} bytes.");
            }

            auto tmp_buffer = static_cast<uint64_t>(bits_buffer >> (sizeof(bits_buffer) - sizeof(uint64_t)) * 8);
            tmp_buffer = byteswap(tmp_buffer);

            memcpy(dest_current, &tmp_buffer, to_write);
            dest_current += to_write;

            bits_buffer <<= to_write * 8;
            bits_count -= to_write * 8;
        }
    };

}
