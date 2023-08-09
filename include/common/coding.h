/*
* Copyright Alibaba Group Holding Ltd.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#pragma once

#include "Root.h"
#include "common/slice.h"

namespace LindormContest {

inline void encode_fixed8_le(uint8_t* buf, uint8_t val) {
    *buf = val;
}

inline void encode_fixed16_le(uint8_t* buf, uint16_t val) {
    memcpy(buf, &val, sizeof(val));
}

inline void encode_fixed32_le(uint8_t* buf, uint32_t val) {
    memcpy(buf, &val, sizeof(val));
}

inline void encode_fixed64_le(uint8_t* buf, uint64_t val) {
    memcpy(buf, &val, sizeof(val));
}

inline uint8_t decode_fixed8_le(const uint8_t* buf) {
    return *buf;
}

inline uint16_t decode_fixed16_le(const uint8_t* buf) {
    uint16_t res;
    memcpy(&res, buf, sizeof(res));
    return res;
}

inline uint32_t decode_fixed32_le(const uint8_t* buf) {
    uint32_t res;
    memcpy(&res, buf, sizeof(res));
    return res;
}

inline uint64_t decode_fixed64_le(const uint8_t* buf) {
    uint64_t res;
    memcpy(&res, buf, sizeof(res));
    return res;
}

template <typename T>
inline void put_fixed32_le(T* dst, uint32_t val) {
    uint8_t buf[sizeof(val)];
    encode_fixed32_le(buf, val);
    dst->append((char*)buf, sizeof(buf));
}

template <typename T>
inline void put_fixed64_le(T* dst, uint64_t val) {
    uint8_t buf[sizeof(val)];
    encode_fixed64_le(buf, val);
    dst->append((char*)buf, sizeof(buf));
}

// Returns the length of the varint32 or varint64 encoding of "v"
inline int varint_length(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

inline uint8_t* encode_varint32(uint8_t* dst, uint32_t v) {
    // Operate on characters as unsigneds
    static const int B = 128;
    if (v < (1 << 7)) {
        *(dst++) = v;
    } else if (v < (1 << 14)) {
        *(dst++) = v | B;
        *(dst++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(dst++) = v | B;
        *(dst++) = (v >> 7) | B;
        *(dst++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(dst++) = v | B;
        *(dst++) = (v >> 7) | B;
        *(dst++) = (v >> 14) | B;
        *(dst++) = v >> 21;
    } else {
        *(dst++) = v | B;
        *(dst++) = (v >> 7) | B;
        *(dst++) = (v >> 14) | B;
        *(dst++) = (v >> 21) | B;
        *(dst++) = v >> 28;
    }
    return dst;
}

inline uint8_t* encode_varint64(uint8_t* dst, uint64_t v) {
    static const unsigned int B = 128;
    while (v >= B) {
        // Fetch low seven bits from current v, and the eight bit is marked as compression mark.
        // v | B is optimised from (v & (B-1)) | B, because result is assigned to uint8_t and other bits
        // is cleared by implicit conversion.
        *(dst++) = v | B;
        v >>= 7;
    }
    *(dst++) = static_cast<unsigned char>(v);
    return dst;
}

inline const uint8_t* decode_varint32_ptr_fallback(const uint8_t* p, const uint8_t* limit,
                                            uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *p;
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

inline const uint8_t* decode_varint32_ptr(const uint8_t* ptr, const uint8_t* limit, uint32_t* value) {
    if (ptr < limit) {
        uint32_t result = *ptr;
        if ((result & 128) == 0) {
            *value = result;
            return ptr + 1;
        }
    }
    return decode_varint32_ptr_fallback(ptr, limit, value);
}

inline const uint8_t* decode_varint64_ptr(const uint8_t* p, const uint8_t* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *p;
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

template <typename T>
inline void put_varint32(T* dst, uint32_t v) {
    uint8_t buf[16];
    uint8_t* ptr = encode_varint32(buf, v);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

template <typename T>
inline void put_varint64(T* dst, uint64_t v) {
    uint8_t buf[16];
    uint8_t* ptr = encode_varint64(buf, v);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}


inline void put_length_prefixed_slice(std::string* dst, const Slice& value) {
    put_varint32(dst, value.size());
    dst->append(reinterpret_cast<const char*>(value.data()), value.size());
}

template <typename T>
inline void put_varint64_varint32(T* dst, uint64_t v1, uint32_t v2) {
    uint8_t buf[16];
    uint8_t* ptr = encode_varint64(buf, v1);
    ptr = encode_varint32(ptr, v2);
    dst->append((char*)buf, static_cast<size_t>(ptr - buf));
}

// parse a varint32 from the start of `input` into `val`.
// on success, return true and advance `input` past the parsed value.
// on failure, return false and `input` is not modified.
inline bool get_varint32(Slice* input, uint32_t* val) {
    const uint8_t* p = (const uint8_t*)input->_data;
    const uint8_t* limit = p + input->_size;
    const uint8_t* q = decode_varint32_ptr(p, limit, val);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

// parse a varint64 from the start of `input` into `val`.
// on success, return true and advance `input` past the parsed value.
// on failure, return false and `input` is not modified.
inline bool get_varint64(Slice* input, uint64_t* val) {
    const uint8_t* p = (const uint8_t*)input->_data;
    const uint8_t* limit = p + input->_size;
    const uint8_t* q = decode_varint64_ptr(p, limit, val);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

// parse a length-prefixed-slice from the start of `input` into `val`.
// on success, return true and advance `input` past the parsed value.
// on failure, return false and `input` may or may not be modified.
inline bool get_length_prefixed_slice(Slice* input, Slice* val) {
    uint32_t len;
    if (get_varint32(input, &len) && input->size() >= len) {
        *val = Slice(input->data(), len);
        input->remove_prefix(len);
        return true;
    } else {
        return false;
    }
}


}