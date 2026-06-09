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

#include "PreparedParameterBinary.h"

#include "Common.h"

namespace iotdb {
namespace prepared {

static void appendInt32BE(std::string& buf, int32_t v) {
  buf.push_back(static_cast<char>((v >> 24) & 0xFF));
  buf.push_back(static_cast<char>((v >> 16) & 0xFF));
  buf.push_back(static_cast<char>((v >> 8) & 0xFF));
  buf.push_back(static_cast<char>(v & 0xFF));
}

static void appendInt64BE(std::string& buf, int64_t v) {
  for (int i = 7; i >= 0; --i) {
    buf.push_back(static_cast<char>((static_cast<uint64_t>(v) >> (i * 8)) & 0xFF));
  }
}

static void appendFloatBE(std::string& buf, float x) {
  static_assert(sizeof(float) == sizeof(uint32_t), "float size");
  uint32_t bits = *reinterpret_cast<const uint32_t*>(&x);
  appendInt32BE(buf, static_cast<int32_t>(bits));
}

static void appendDoubleBE(std::string& buf, double x) {
  static_assert(sizeof(double) == sizeof(uint64_t), "double size");
  uint64_t bits = *reinterpret_cast<const uint64_t*>(&x);
  appendInt64BE(buf, static_cast<int64_t>(bits));
}

static void appendByte(std::string& buf, uint8_t b) {
  buf.push_back(static_cast<char>(b));
}

static void appendTsDataType(std::string& buf, TSDataType::TSDataType t) {
  appendByte(buf, static_cast<uint8_t>(t));
}

static void appendStringUtf8(std::string& buf, const std::string& s) {
  appendInt32BE(buf, static_cast<int32_t>(s.size()));
  buf.append(s);
}

std::string serializeParameters(const std::vector<ParamSlot>& params) {
  std::string out;
  appendInt32BE(out, static_cast<int32_t>(params.size()));
  for (const ParamSlot& p : params) {
    switch (p.kind) {
    case ParamKind::kNull:
      appendTsDataType(out, TSDataType::UNKNOWN);
      break;
    case ParamKind::kBool:
      appendTsDataType(out, TSDataType::BOOLEAN);
      appendByte(out, p.boolVal ? 1 : 0);
      break;
    case ParamKind::kInt32:
      appendTsDataType(out, TSDataType::INT32);
      appendInt32BE(out, p.int32Val);
      break;
    case ParamKind::kInt64:
      appendTsDataType(out, TSDataType::INT64);
      appendInt64BE(out, p.int64Val);
      break;
    case ParamKind::kFloat:
      appendTsDataType(out, TSDataType::FLOAT);
      appendFloatBE(out, p.floatVal);
      break;
    case ParamKind::kDouble:
      appendTsDataType(out, TSDataType::DOUBLE);
      appendDoubleBE(out, p.doubleVal);
      break;
    case ParamKind::kString:
      appendTsDataType(out, TSDataType::STRING);
      appendStringUtf8(out, p.stringOrBlob);
      break;
    case ParamKind::kBlob:
      appendTsDataType(out, TSDataType::BLOB);
      appendInt32BE(out, static_cast<int32_t>(p.stringOrBlob.size()));
      out.append(p.stringOrBlob);
      break;
    }
  }
  return out;
}

} // namespace prepared
} // namespace iotdb
