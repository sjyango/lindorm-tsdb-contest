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

namespace LindormContest::util {

inline void memcpy_small_allow_read_write_overflow15_impl(char* __restrict dst,
                                                          const char* __restrict src, ssize_t n) {
    while (n > 0) {
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst),
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(src)));

        dst += 16;
        src += 16;
        n -= 16;
    }
}

/** Works under assumption, that it's possible to read up to 15 excessive bytes after end of 'src' region
  *  and to write any garbage into up to 15 bytes after end of 'dst' region.
  */
inline void memcpy_small_allow_read_write_overflow15(void* __restrict dst,
                                                     const void* __restrict src, size_t n) {
    util::memcpy_small_allow_read_write_overflow15_impl(reinterpret_cast<char*>(dst),
                                                          reinterpret_cast<const char*>(src), n);
}

}
