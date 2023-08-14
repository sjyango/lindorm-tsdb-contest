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

#include <unordered_map>

#include "Root.h"
#include "struct/ColumnValue.h"

namespace LindormContest::storage {

using EncodeAscendingFunc = void (*)(const void* value, std::string* buf);
using DecodeAscendingFunc = void (*)(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr);

static std::string padding_format(int64_t value, size_t width) {
    std::string str_value = std::to_string(value);
    if (str_value.length() < width) {
        str_value = std::string(width - str_value.length(), '0') + str_value;
    }
    return str_value;
}

// static std::string padding_format(uint64_t value, size_t width) {
//     std::string str_value = std::to_string(value);
//     if (str_value.length() < width) {
//         str_value = std::string(width - str_value.length(), '0') + str_value;
//     }
//     return str_value;
// }

// Order-preserving binary encoding for values of a particular type so that
// those values can be compared by memcpy their encoded bytes.
//
// To obtain instance of this class, use the `get_key_coder(FieldType)` method.
class KeyCoder {
public:
    template <typename TraitsType>
    KeyCoder(TraitsType traits)
            : _decode_ascending(traits.decode_ascending),
              _encode_ascending(traits.encode_ascending) {}

    void encode_ascending(const void* value, std::string* buf) const {
        _encode_ascending(value, buf);
    }

    void decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) const {
        return _decode_ascending(encoded_key, index_size, cell_ptr);
    }

private:
    DecodeAscendingFunc _decode_ascending;
    EncodeAscendingFunc _encode_ascending;
};

template <ColumnType column_type>
struct KeyCoderTraits {};

template <>
struct KeyCoderTraits<ColumnType::COLUMN_TYPE_INTEGER> {
    using KeyType = Int32;

    static void encode_ascending(const void* value, std::string* buf) {
        KeyType key_val;
        std::memcpy(&key_val, value, sizeof(KeyType));
        buf->append((char*) &key_val, sizeof(KeyType));
    }

    static void decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        KeyType key_val;
        memcpy(&key_val, encoded_key->_data, sizeof(KeyType));
        memcpy(cell_ptr, &key_val, sizeof(KeyType));
        encoded_key->remove_prefix(sizeof(KeyType));
    }
};

template <>
struct KeyCoderTraits<ColumnType::COLUMN_TYPE_TIMESTAMP> {
    using KeyType = Int64;

    // encoded(37) = vin(17) + timestamp(20)
    static void encode_ascending(const void* value, std::string* buf) {
        KeyType key_val;
        std::memcpy(&key_val, value, sizeof(KeyType));
        std::string encoded_str = padding_format(key_val, 20);
        buf->append(encoded_str);
    }

    static void decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        KeyType key_val;
        memcpy(&key_val, encoded_key->_data, sizeof(KeyType));
        memcpy(cell_ptr, &key_val, sizeof(KeyType));
        encoded_key->remove_prefix(sizeof(KeyType));
    }
};

template <>
struct KeyCoderTraits<ColumnType::COLUMN_TYPE_DOUBLE_FLOAT> {
    using KeyType = Float64;

    static void encode_ascending(const void* value, std::string* buf) {
        KeyType key_val;
        std::memcpy(&key_val, value, sizeof(KeyType));
        buf->append((char*) &key_val, sizeof(KeyType));
    }

    static void decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        KeyType key_val;
        memcpy(&key_val, encoded_key->_data, sizeof(KeyType));
        memcpy(cell_ptr, &key_val, sizeof(KeyType));
        encoded_key->remove_prefix(sizeof(KeyType));
    }
};

template <>
struct KeyCoderTraits<ColumnType::COLUMN_TYPE_STRING> {
    using KeyType = Slice;

    static void encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->_data, slice->_size);
    }

    static void decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        ERR_LOG("decode_ascending is not implemented")
        throw std::runtime_error("decode_ascending is not implemented");
    }
};

template <>
struct KeyCoderTraits<ColumnType::COLUMN_TYPE_UNINITIALIZED> {
    using KeyType = uint64_t;

    static void encode_ascending(const void* value, std::string* buf) {
        KeyType key_val;
        std::memcpy(&key_val, value, sizeof(KeyType));
        buf->append((char*) &key_val, sizeof(KeyType));
    }

    static void decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        KeyType key_val;
        memcpy(&key_val, encoded_key->_data, sizeof(KeyType));
        memcpy(cell_ptr, &key_val, sizeof(KeyType));
        encoded_key->remove_prefix(sizeof(KeyType));
    }
};


// Helper class used to get KeyCoder
class KeyCoderFactory {
public:
    ~KeyCoderFactory() {
        for (auto& iter : _coder_map) {
            delete iter.second;
        }
    }

    static KeyCoderFactory& instance() {
        static KeyCoderFactory s_instance;
        return s_instance;
    }

    KeyCoder* get_coder(ColumnType column_type) const {
        auto it = _coder_map.find(column_type);
        if (it != _coder_map.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    KeyCoderFactory() {
        _coder_map.emplace(COLUMN_TYPE_INTEGER, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_INTEGER>()));
        _coder_map.emplace(COLUMN_TYPE_TIMESTAMP, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_TIMESTAMP>()));
        _coder_map.emplace(COLUMN_TYPE_DOUBLE_FLOAT, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_DOUBLE_FLOAT>()));
        _coder_map.emplace(COLUMN_TYPE_STRING, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_STRING>()));
    }

    struct EnumClassHash {
        template <typename T>
        std::size_t operator()(T t) const {
            return static_cast<std::size_t>(t);
        }
    };

    std::unordered_map<ColumnType, KeyCoder*, EnumClassHash> _coder_map;
};

inline const KeyCoder* get_key_coder(ColumnType type) {
    return KeyCoderFactory::instance().get_coder(type);
}

}