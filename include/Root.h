//
// Don't modify this file, the evaluation program is compiled
// based on this header file.
//

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

#include <set>
#include <vector>
#include <cstring>
#include <string>
#include <utility>
#include <ostream>
#include <map>
#include <cstdio>
#include <iostream>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <cassert>
#include <memory>

namespace LindormContest {

using Path = std::filesystem::path;

static constexpr size_t SCHEMA_COLUMN_NUMS = 3;

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define ERR_LOG(str, ...) {                                  \
    fprintf(stderr, "%s:%d. [ERROR]: ", __FILE__, __LINE__); \
    fprintf(stderr, str, ##__VA_ARGS__);                     \
    fprintf(stderr, "\n");                                   \
}

#define INFO_LOG(str, ...) {                                 \
    fprintf(stdout, "%s:%d. [INFO]: ", __FILE__, __LINE__);  \
    fprintf(stdout, str, ##__VA_ARGS__);                     \
    fprintf(stdout, "\n");                                   \
}

}