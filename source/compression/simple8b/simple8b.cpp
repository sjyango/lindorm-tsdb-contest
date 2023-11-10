#include "compression/simple8b/simple8b.h"

#include <cstring>
#include <stdexcept>

#include "common/osLz4.h"

#define SIMPLE8B_MAX_INT64 ((uint64_t)1152921504606846974LL)

#define safeInt64Add(a, b)  (((a >= 0) && (b <= INT64_MAX - a)) || ((a < 0) && (b >= INT64_MIN - a)))
#define ZIGZAG_ENCODE(T, v) (((u##T)((v) >> (sizeof(T) * 8 - 1))) ^ (((u##T)(v)) << 1))  // zigzag encode
#define ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))                                 // zigzag decode

/*
* Compress Integer (Simple8B).
*/
int32_t tsCompressINTImp(const char *const input, const int32_t nelements, char *const output) {
    // Selector value:              0    1   2   3   4   5   6   7   8  9  10  11
    // 12  13  14  15
    char bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
    int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};
    char bit_to_selector[] = {0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
                              14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                              15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15};

    // get the byte limit.
    int32_t word_length = sizeof(int32_t);
    uint64_t buffer = 0;

    int32_t byte_limit = nelements * word_length + 1;
    int32_t opos = 1;
    int64_t prev_value = 0;

    for (int32_t i = 0; i < nelements;) {
        char selector = 0;
        char bit = 0;
        int32_t elems = 0;
        int64_t prev_value_tmp = prev_value;

        for (int32_t j = i; j < nelements; j++) {
            // Read data from the input stream and convert it to INT64 type.
            int64_t curr_value = (int64_t) (*((int32_t *) input + j));
            // Get difference.
            if (!safeInt64Add(curr_value, -prev_value_tmp)) goto _copy_and_exit;

            int64_t diff = curr_value - prev_value_tmp;
            // Zigzag encode the value.
            uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);

            if (zigzag_value >= SIMPLE8B_MAX_INT64) goto _copy_and_exit;

            int64_t tmp_bit;
            if (zigzag_value == 0) {
                // Take care here, __builtin_clzl give wrong anser for value 0;
                tmp_bit = 0;
            } else {
                tmp_bit = (sizeof(int64_t) * BITS_PER_BYTE) - BUILDIN_CLZL(zigzag_value);
            }

            if (elems + 1 <= selector_to_elems[(int32_t) selector] &&
                elems + 1 <= selector_to_elems[(int32_t) (bit_to_selector[(int32_t) tmp_bit])]) {
                // If can hold another one.
                selector =
                        selector > bit_to_selector[(int32_t) tmp_bit] ? selector : bit_to_selector[(int32_t) tmp_bit];
                elems++;
                bit = bit_per_integer[(int32_t) selector];
            } else {
                // if cannot hold another one.
                while (elems < selector_to_elems[(int32_t) selector]) selector++;
                elems = selector_to_elems[(int32_t) selector];
                bit = bit_per_integer[(int32_t) selector];
                break;
            }
            prev_value_tmp = curr_value;
        }

        buffer = 0;
        buffer |= (uint64_t) selector;
        for (int32_t k = 0; k < elems; k++) {
            int64_t curr_value = (int64_t) (*((int32_t *) input + i)); /* get current values */
            int64_t diff = curr_value - prev_value;
            uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);
            buffer |= ((zigzag_value & INT64MASK(bit)) << (bit * k + 4));
            i++;
            prev_value = curr_value;
        }

        // Output the encoded value to the output.
        if (opos + sizeof(buffer) <= byte_limit) {
            memcpy(output + opos, &buffer, sizeof(buffer));
            opos += sizeof(buffer);
        } else {
            _copy_and_exit:
            output[0] = 1;
            memcpy(output + 1, input, byte_limit - 1);
            return byte_limit;
        }
    }

    // set the indicator.
    output[0] = 0;
    return opos;
}


int32_t tsDecompressINTImp(const char *const input, const int32_t nelements, char *const output) {

    int32_t word_length = 4;

    // If not compressed.
    if (input[0] == 1) {
        memcpy(output, input + 1, nelements * word_length);
        return nelements * word_length;
    }

    // Selector value:              0    1   2   3   4   5   6   7   8  9  10  11
    // 12  13  14  15
    char bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
    int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

    const char *ip = input + 1;
    int32_t count = 0;
    int32_t _pos = 0;
    int64_t prev_value = 0;

    while (true) {
        if (count == nelements) break;

        uint64_t w = 0;
        memcpy(&w, ip, 8);

        char selector = (char) (w & INT64MASK(4));       // selector = 4
        char bit = bit_per_integer[(int32_t) selector];  // bit = 3
        int32_t elems = selector_to_elems[(int32_t) selector];

        for (int32_t i = 0; i < elems; i++) {
            uint64_t zigzag_value;

            if (selector == 0 || selector == 1) {
                zigzag_value = 0;
            } else {
                zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
            }
            int64_t diff = ZIGZAG_DECODE(int64_t, zigzag_value);
            int64_t curr_value = diff + prev_value;
            prev_value = curr_value;

            *((int32_t *) output + _pos) = (int32_t) curr_value;
            _pos++;
            count++;
            if (count == nelements) break;
        }
        ip += 8;
    }

    return nelements * word_length;
}

/* ----------------------------------------------Bool Compression
* ---------------------------------------------- */

/* Run Length Encoding(RLE) Method */
/*Format: | value[8B] | repeat_N[8B] | ... |*/
int32_t tsCompressRLEImp(const char *const input, const int32_t nelements, char *const output) {
    int32_t _pos = 0;

    for (int32_t i = 0; i < nelements;) {
        uint64_t counter = 1;
        int32_t num = *((int32_t *) input + i);

        for (++i; i < nelements; i++) {
            if ((*((int32_t *) input + i)) == num) {
                counter++;
            } else {
                break;
            }
        }
        // Encode the data.
        int64_t value = int64_t(num);
        memcpy(output + _pos, &value, sizeof(int64_t));
        memcpy(output + _pos + sizeof(int64_t), &counter, sizeof(uint64_t));
        _pos += sizeof(int64_t) + sizeof(uint64_t);

    }

    return _pos;
}

int32_t tsDecompressRLEImp(const char *const input, const int32_t nelements, char *const output) {
    int32_t ipos = 0, opos = 0;
    const char *base = input;
    int32_t *output_ = (int32_t *) output;

    while (true) {
        int64_t value = 0;
        memcpy(&value, base + ipos, 8);
        int32_t num = int32_t(value);

        uint64_t counter = 0;
        memcpy(&counter, base + ipos + 8, 8);

        std::fill(output_ + opos, output_ + opos + counter, num);
        opos += counter;
        if (opos >= nelements) {
            return nelements;
        }
        ipos += 16;
    }
}