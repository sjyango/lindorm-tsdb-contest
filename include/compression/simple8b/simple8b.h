/*
* Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
*
* This program is free software: you can use, redistribute, and/or modify
* it under the terms of the GNU Affero General Public License, version 3
* or later ("AGPL"), as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
* FITNESS FOR A PARTICULAR PURPOSE.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#pragma once

#include <cstdint>
#ifdef __cplusplus
extern "C" {
#endif

#define COMP_OVERFLOW_BYTES 2
#define BITS_PER_BYTE       8
// Masks
#define INT64MASK(_x) ((((uint64_t)1) << _x) - 1)
#define INT32MASK(_x) (((uint32_t)1 << _x) - 1)
#define INT8MASK(_x)  (((uint8_t)1 << _x) - 1)
// Compression algorithm
#define NO_COMPRESSION 0
#define ONE_STAGE_COMP 1
#define TWO_STAGE_COMP 2

/*************************************************************************
*                  REGULAR COMPRESSION
*************************************************************************/

int32_t tsCompressINTImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressINTImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsCompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output);

#ifdef __cplusplus
}
#endif