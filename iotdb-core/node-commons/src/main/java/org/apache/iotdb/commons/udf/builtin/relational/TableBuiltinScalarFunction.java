/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.udf.builtin.relational;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableBuiltinScalarFunction {
  DIFF("diff"),
  CAST("cast"),
  ROUND("round"),
  REPLACE("replace"),
  SUBSTRING("substring"),
  LENGTH("length"),
  UPPER("upper"),
  LOWER("lower"),
  TRIM("trim"),
  LTRIM("ltrim"),
  RTRIM("rtrim"),
  REGEXP_LIKE("regexp_like"),
  STRPOS("strpos"),
  STARTS_WITH("starts_with"),
  ENDS_WITH("ends_with"),
  CONCAT("concat"),
  STRCMP("strcmp"),
  SIN("sin"),
  COS("cos"),
  TAN("tan"),
  ASIN("asin"),
  ACOS("acos"),
  ATAN("atan"),
  SINH("sinh"),
  COSH("cosh"),
  TANH("tanh"),
  DEGREES("degrees"),
  RADIANS("radians"),
  ABS("abs"),
  SIGN("sign"),
  CEIL("ceil"),
  FLOOR("floor"),
  EXP("exp"),
  LN("ln"),
  LOG10("log10"),
  SQRT("sqrt"),
  PI("pi"),
  E("e"),
  DATE_BIN("date_bin"),
  FORMAT("format"),
  GREATEST("greatest"),
  LEAST("least"),
  BIT_COUNT("bit_count"),
  BITWISE_AND("bitwise_and"),
  BITWISE_NOT("bitwise_not"),
  BITWISE_OR("bitwise_or"),
  BITWISE_XOR("bitwise_xor"),
  BITWISE_LEFT_SHIFT("bitwise_left_shift"),
  BITWISE_RIGHT_SHIFT("bitwise_right_shift"),
  BITWISE_RIGHT_SHIFT_ARITHMETIC("bitwise_right_shift_arithmetic"),
  TO_BASE64("to_base64"),
  FROM_BASE64("from_base64"),
  TO_BASE64URL("to_base64url"),
  FROM_BASE64URL("from_base64url"),
  TO_BASE32("to_base32"),
  FROM_BASE32("from_base32"),
  SHA256("sha256"),
  SHA512("sha512"),
  SHA1("sha1"),
  MD5("md5"),
  XXHASH64("xxhash64"),
  MURMUR3("murmur3"),
  TO_HEX("to_hex"),
  FROM_HEX("from_hex"),
  REVERSE("reverse"),
  HMAC_MD5("hmac_md5"),
  HMAC_SHA1("hmac_sha1"),
  HMAC_SHA256("hmac_sha256"),
  HMAC_SHA512("hmac_sha512"),
  TO_BIG_ENDIAN_32("to_big_endian_32"),
  FROM_BIG_ENDIAN_32("from_big_endian_32"),
  TO_BIG_ENDIAN_64("to_big_endian_64"),
  FROM_BIG_ENDIAN_64("from_big_endian_64"),
  TO_LITTLE_ENDIAN_32("to_little_endian_32"),
  FROM_LITTLE_ENDIAN_32("from_little_endian_32"),
  TO_LITTLE_ENDIAN_64("to_little_endian_64"),
  FROM_LITTLE_ENDIAN_64("from_little_endian_64"),
  TO_IEEE754_32("to_ieee754_32"),
  FROM_IEEE754_32("from_ieee754_32"),
  TO_IEEE754_64("to_ieee754_64"),
  FROM_IEEE754_64("from_ieee754_64"),
  CRC32("crc32"),
  SPOOKY_HASH_V2_32("spooky_hash_v2_32"),
  SPOOKY_HASH_V2_64("spooky_hash_v2_64"),
  LPAD("lpad"),
  RPAD("rpad"),
  ;

  private final String functionName;

  TableBuiltinScalarFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> BUILT_IN_SCALAR_FUNCTION_NAME =
      new HashSet<>(
          Arrays.stream(TableBuiltinScalarFunction.values())
              .map(TableBuiltinScalarFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getBuiltInScalarFunctionName() {
    return BUILT_IN_SCALAR_FUNCTION_NAME;
  }
}
