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
    char    bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
    int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};
    char    bit_to_selector[] = {0,  2,  3,  4,  5,  6,  7,  8,  9,  10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
                              14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                              15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15};

    // get the byte limit.
    int32_t word_length = sizeof(int32_t);
    uint64_t buffer = 0;

    int32_t byte_limit = nelements * word_length + 1;
    int32_t opos = 1;
    int64_t prev_value = 0;

    for (int32_t i = 0; i < nelements;) {
        char    selector = 0;
        char    bit = 0;
        int32_t elems = 0;
        int64_t prev_value_tmp = prev_value;

        for (int32_t j = i; j < nelements; j++) {
            // Read data from the input stream and convert it to INT64 type.
            int64_t curr_value = (int64_t)(*((int32_t *)input + j));
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

            if (elems + 1 <= selector_to_elems[(int32_t)selector] &&
                elems + 1 <= selector_to_elems[(int32_t)(bit_to_selector[(int32_t)tmp_bit])]) {
                // If can hold another one.
                selector = selector > bit_to_selector[(int32_t)tmp_bit] ? selector : bit_to_selector[(int32_t)tmp_bit];
                elems++;
                bit = bit_per_integer[(int32_t)selector];
            } else {
                // if cannot hold another one.
                while (elems < selector_to_elems[(int32_t)selector]) selector++;
                elems = selector_to_elems[(int32_t)selector];
                bit = bit_per_integer[(int32_t)selector];
                break;
            }
            prev_value_tmp = curr_value;
        }

        buffer = 0;
        buffer |= (uint64_t)selector;
        for (int32_t k = 0; k < elems; k++) {
            int64_t curr_value = (int64_t)(*((int32_t *)input + i)); /* get current values */
            int64_t  diff = curr_value - prev_value;
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
   char    bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
   int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

   const char *ip = input + 1;
   int32_t     count = 0;
   int32_t     _pos = 0;
   int64_t     prev_value = 0;

#if __AVX2__
   while (1) {
       if (_pos == nelements) break;

       uint64_t w = 0;
       memcpy(&w, ip, 8);

       char    selector = (char)(w & INT64MASK(4));       // selector = 4
       char    bit = bit_per_integer[(int32_t)selector];  // bit = 3
       int32_t elems = selector_to_elems[(int32_t)selector];

       // Optimize the performance, by remove the constantly switch operation.
       int32_t  v = 4;
       uint64_t zigzag_value = 0;
       uint64_t mask = INT64MASK(bit);

       switch (type) {
       case TSDB_DATA_TYPE_BIGINT: {
           int64_t* p = (int64_t*) output;

           int32_t gRemainder = (nelements - _pos);
           int32_t num = (gRemainder > elems)? elems:gRemainder;

           int32_t batch = num >> 2;
           int32_t remain = num & 0x03;
           if (selector == 0 || selector == 1) {
               if (tsAVX2Enable && tsSIMDBuiltins) {
                   for (int32_t i = 0; i < batch; ++i) {
                       __m256i prev = _mm256_set1_epi64x(prev_value);
                       _mm256_storeu_si256((__m256i *)&p[_pos], prev);
                       _pos += 4;
                   }

                   for (int32_t i = 0; i < remain; ++i) {
                       p[_pos++] = prev_value;
                   }
               } else {
                   for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                       p[_pos++] = prev_value;
                       v += bit;
                   }
               }
           } else {
               if (tsAVX2Enable && tsSIMDBuiltins) {
                   __m256i base = _mm256_set1_epi64x(w);
                   __m256i maskVal = _mm256_set1_epi64x(mask);

                   __m256i shiftBits = _mm256_set_epi64x(bit * 3 + 4, bit * 2 + 4, bit + 4, 4);
                   __m256i inc = _mm256_set1_epi64x(bit << 2);

                   for (int32_t i = 0; i < batch; ++i) {
                       __m256i after = _mm256_srlv_epi64(base, shiftBits);
                       __m256i zigzagVal = _mm256_and_si256(after, maskVal);

                       // ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))
                       __m256i signmask = _mm256_and_si256(_mm256_set1_epi64x(1), zigzagVal);
                       signmask = _mm256_sub_epi64(_mm256_setzero_si256(), signmask);
                       // get the four zigzag values here
                       __m256i delta = _mm256_xor_si256(_mm256_srli_epi64(zigzagVal, 1), signmask);

                       // calculate the cumulative sum (prefix sum) for each number
                       // decode[0] = prev_value + final[0]
                       // decode[1] = decode[0] + final[1]   -----> prev_value + final[0] + final[1]
                       // decode[2] = decode[1] + final[2]   -----> prev_value + final[0] + final[1] + final[2]
                       // decode[3] = decode[2] + final[3]   -----> prev_value + final[0] + final[1] + final[2] + final[3]

                       //  1, 2, 3, 4
                       //+ 0, 1, 0, 3
                       //  1, 3, 3, 7
                       // shift and add for the first round
                       __m128i prev = _mm_set1_epi64x(prev_value);
                       __m256i x =  _mm256_slli_si256(delta, 8);

                       delta = _mm256_add_epi64(delta, x);
                       _mm256_storeu_si256((__m256i *)&p[_pos], delta);

                       //  1, 3, 3, 7
                       //+ 0, 0, 3, 3
                       //  1, 3, 6, 10
                       // shift and add operation for the second round
                       __m128i firstPart = _mm_loadu_si128((__m128i *)&p[_pos]);
                       __m128i secondItem = _mm_set1_epi64x(p[_pos + 1]);
                       __m128i secPart = _mm_add_epi64(_mm_loadu_si128((__m128i *)&p[_pos + 2]), secondItem);
                       firstPart = _mm_add_epi64(firstPart, prev);
                       secPart = _mm_add_epi64(secPart, prev);

                       // save it in the memory
                       _mm_storeu_si128((__m128i *)&p[_pos], firstPart);
                       _mm_storeu_si128((__m128i *)&p[_pos + 2], secPart);

                       shiftBits = _mm256_add_epi64(shiftBits, inc);
                       prev_value = p[_pos + 3];
                       //              uDebug("_pos:%d %"PRId64", %"PRId64", %"PRId64", %"PRId64, _pos, p[_pos], p[_pos+1], p[_pos+2], p[_pos+3]);
                       _pos += 4;
                   }

                   // handle the remain value
                   for (int32_t i = 0; i < remain; i++) {
                       zigzag_value = ((w >> (v + (batch * bit * 4))) & mask);
                       prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

                       p[_pos++] = prev_value;
                       //              uDebug("_pos:%d %"PRId64, _pos-1, p[_pos-1]);

                       v += bit;
                   }
               } else {
                   for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                       zigzag_value = ((w >> v) & mask);
                       prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

                       p[_pos++] = prev_value;
                       //              uDebug("_pos:%d %"PRId64, _pos-1, p[_pos-1]);

                       v += bit;
                   }
               }
           }
       } break;
       case TSDB_DATA_TYPE_INT: {
           int32_t* p = (int32_t*) output;

           if (selector == 0 || selector == 1) {
               for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                   p[_pos++] = (int32_t)prev_value;
               }
           } else {
               for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                   zigzag_value = ((w >> v) & mask);
                   prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

                   p[_pos++] = (int32_t)prev_value;
                   v += bit;
               }
           }
       } break;
       case TSDB_DATA_TYPE_SMALLINT: {
           int16_t* p = (int16_t*) output;

           if (selector == 0 || selector == 1) {
               for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                   p[_pos++] = (int16_t)prev_value;
               }
           } else {
               for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                   zigzag_value = ((w >> v) & mask);
                   prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

                   p[_pos++] = (int16_t)prev_value;
                   v += bit;
               }
           }
       } break;

       case TSDB_DATA_TYPE_TINYINT: {
           int8_t *p = (int8_t *)output;

           if (selector == 0 || selector == 1) {
               for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                   p[_pos++] = (int8_t)prev_value;
               }
           } else {
               for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
                   zigzag_value = ((w >> v) & mask);
                   prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

                   p[_pos++] = (int8_t)prev_value;
                   v += bit;
               }
           }
       } break;
       }

       ip += 8;
   }

   return nelements * word_length;
#else

   while (true) {
       if (count == nelements) break;

       uint64_t w = 0;
       memcpy(&w, ip, 8);

       char    selector = (char)(w & INT64MASK(4));       // selector = 4
       char    bit = bit_per_integer[(int32_t)selector];  // bit = 3
       int32_t elems = selector_to_elems[(int32_t)selector];

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

           *((int32_t *)output + _pos) = (int32_t)curr_value;
           _pos++;
           count++;
           if (count == nelements) break;
       }
       ip += 8;
   }

   return nelements * word_length;
#endif
}

/* ----------------------------------------------Bool Compression
* ---------------------------------------------- */

/* Run Length Encoding(RLE) Method */
int32_t tsCompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output) {
 int32_t _pos = 0;

 for (int32_t i = 0; i < nelements;) {
   unsigned char counter = 1;
   char          num = input[i];

   for (++i; i < nelements; i++) {
     if (input[i] == num) {
       counter++;
       if (counter == INT8MASK(7)) {
         i++;
         break;
       }
     } else {
       break;
     }
   }

   // Encode the data.
   if (num == 1) {
     output[_pos++] = INT8MASK(1) | (counter << 1);
   } else if (num == 0) {
     output[_pos++] = (counter << 1) | INT8MASK(0);
   } else {
     throw std::runtime_error("Invalid compress bool value");
   }
 }

 return _pos;
}

int32_t tsDecompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output) {
 int32_t ipos = 0, opos = 0;
 while (true) {
   char     encode = input[ipos++];
   unsigned counter = (encode >> 1) & INT8MASK(7);
   char     value = encode & INT8MASK(1);

   memset(output + opos, value, counter);
   opos += counter;
   if (opos >= nelements) {
     return nelements;
   }
 }
}