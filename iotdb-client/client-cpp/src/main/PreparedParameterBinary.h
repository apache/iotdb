/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef IOTDB_PREPARED_PARAMETER_BINARY_H
#define IOTDB_PREPARED_PARAMETER_BINARY_H

#include <cstdint>
#include <string>
#include <vector>

namespace iotdb {
namespace prepared {

enum class ParamKind { kNull, kBool, kInt32, kInt64, kFloat, kDouble, kString, kBlob };

struct ParamSlot {
    ParamKind kind{ParamKind::kNull};
    /** Primitive payload; active member is determined by `kind`. */
    union {
        bool boolVal;
        int32_t int32Val;
        int64_t int64Val;
        float floatVal;
        double doubleVal;
    };
    /** UTF-8 for kString; raw bytes for kBlob (may contain embedded NULs). */
    std::string stringOrBlob;
};

std::string serializeParameters(const std::vector<ParamSlot>& params);

}  // namespace prepared
}  // namespace iotdb

#endif
