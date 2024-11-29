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

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator;

import org.apache.tsfile.read.common.type.Type;

public class JoinKeyComparatorFactory {

  public static JoinKeyComparator getComparator(Type type, boolean isAscending) {
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
        return isAscending
            ? AscIntTypeJoinKeyComparator.getInstance()
            : DescIntTypeJoinKeyComparator.getInstance();
      case INT64:
      case TIMESTAMP:
        return isAscending
            ? AscLongTypeJoinKeyComparator.getInstance()
            : DescLongTypeJoinKeyComparator.getInstance();
      case FLOAT:
        return isAscending
            ? AscFloatTypeJoinKeyComparator.getInstance()
            : DescFloatTypeJoinKeyComparator.getInstance();
      case DOUBLE:
        return isAscending
            ? AscDoubleTypeJoinKeyComparator.getInstance()
            : DescDoubleTypeJoinKeyComparator.getInstance();
      case BOOLEAN:
        return isAscending
            ? AscBooleanTypeJoinKeyComparator.getInstance()
            : DescBooleanTypeJoinKeyComparator.getInstance();
      case STRING:
      case BLOB:
      case TEXT:
        return isAscending
            ? AscBinaryTypeJoinKeyComparator.getInstance()
            : DescBinaryTypeJoinKeyComparator.getInstance();
      default:
        // other types are not supported.
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }
}
