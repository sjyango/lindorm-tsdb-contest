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
#include "common/status.h"
#include "struct/ColumnValue.h"

namespace LindormContest::storage {

using FullEncodeAscendingFunc = void (*)(const void* value, std::string* buf);
using EncodeAscendingFunc = void (*)(const void* value, std::string* buf);
using DecodeAscendingFunc = Status (*)(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr);

// Order-preserving binary encoding for values of a particular type so that
// those values can be compared by memcpy their encoded bytes.
//
// To obtain instance of this class, use the `get_key_coder(FieldType)` method.
class KeyCoder {
public:
    template <typename TraitsType>
    KeyCoder(TraitsType traits)
            : _full_encode_ascending(traits.full_encode_ascending),
              _encode_ascending(traits.encode_ascending),
              _decode_ascending(traits.decode_ascending) {}

    // encode the provided `value` into `buf`.
    void full_encode_ascending(const void* value, std::string* buf) const {
        _full_encode_ascending(value, buf);
    }

    // similar to `full_encode_ascending`, but only encode part (the first `index_size` bytes) of the value.
    // only applicable to string type
    void encode_ascending(const void* value, std::string* buf) const {
        _encode_ascending(value, buf);
    }

    // Only used for test, should delete it in the future
    Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) const {
        return _decode_ascending(encoded_key, index_size, cell_ptr);
    }

private:
    FullEncodeAscendingFunc _full_encode_ascending;
    EncodeAscendingFunc _encode_ascending;
    DecodeAscendingFunc _decode_ascending;
};

template <ColumnType column_type>
struct KeyCoderTraits {};

template <>
struct KeyCoderTraits<ColumnType::COLUMN_TYPE_INTEGER> {
    using KeyType = Int32;

    static void full_encode_ascending(const void* value, std::string* buf) {
        KeyType key_val;
        std::memcpy(&key_val, value, sizeof(KeyType));
        buf->append((char*) &key_val, sizeof(KeyType));
    }

    static void encode_ascending(const void* value, std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        // decode_ascending only used in orinal index page, maybe should remove it in the future.
        // currently, we reduce the usage of this method.
        if (encoded_key->_size < sizeof(KeyType)) {
            return Status::InvalidArgument("Key is too short");
        }
        KeyType key_val;
        memcpy(&key_val, encoded_key->_data, sizeof(KeyType));
        memcpy(cell_ptr, &key_val, sizeof(KeyType));
        encoded_key->remove_prefix(sizeof(KeyType));
        return Status::OK();
    }
};

template <>
struct KeyCoderTraits<ColumnType::COLUMN_TYPE_STRING> {
    using KeyType = Slice;

    static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->_data, slice->_size);
    }

    static void encode_ascending(const void* value, std::string* buf) {
        const Slice* slice = (const Slice*)value;
        size_t copy_size = std::min(sizeof(Slice), slice->_size);
        buf->append(slice->_data, copy_size);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr) {
        return Status::NotSupported("decode_ascending is not implemented");
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

    KeyCoder* get_coder(ColumnType field_type) const {
        auto it = _coder_map.find(field_type);
        if (it != _coder_map.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    KeyCoderFactory() {
        _coder_map.emplace(COLUMN_TYPE_INTEGER, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_INTEGER>()));
        // _coder_map.emplace(COLUMN_TYPE_TIMESTAMP, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_INTEGER>()));
        // _coder_map.emplace(COLUMN_TYPE_DOUBLE_FLOAT, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_INTEGER>()));
        _coder_map.emplace(COLUMN_TYPE_STRING, new KeyCoder(KeyCoderTraits<COLUMN_TYPE_INTEGER>()));
    }

    struct EnumClassHash {
        template <typename T>
        std::size_t operator()(T t) const {
            return static_cast<std::size_t>(t);
        }
    };

    std::unordered_map<ColumnType, KeyCoder*, EnumClassHash> _coder_map;
};

const KeyCoder* get_key_coder(ColumnType type) {
    return KeyCoderFactory::instance().get_coder(type);
}

}