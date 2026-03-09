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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.util;

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isBlobType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isCharType;

/**
 * A utility class for generating user-friendly debug information during the execution of
 * ColumnTransformers, especially for error reporting.
 */
public final class TransformerDebugUtils {

  private TransformerDebugUtils() {}

  /**
   * Generates a user-friendly string representation of a raw byte array based on its original data
   * type. This is primarily used for creating informative error messages when a decoding operation
   * fails.
   */
  public static String generateOriginalValue(byte[] inputBytes, Type originalType) {

    // If the original type was character-based, interpret it as a string.
    if (isCharType(originalType)) {
      return new String(inputBytes, TSFileConfig.STRING_CHARSET);
    }

    // If the original type was BLOB, represent it in hexadecimal format.
    if (isBlobType(originalType)) {
      StringBuilder hexString = new StringBuilder("0x");
      for (byte inputByte : inputBytes) {
        hexString.append(String.format("%02x", inputByte));
      }
      return hexString.toString();
    }

    throw new SemanticException(
        "type " + originalType + " is not supported in generateProblematicValueString()");
  }
}
