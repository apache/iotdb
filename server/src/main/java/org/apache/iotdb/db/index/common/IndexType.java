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
package org.apache.iotdb.db.index.common;

import org.apache.iotdb.db.exception.index.UnsupportedIndexTypeException;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

public enum IndexType {
  NO_INDEX,
  RTREE_PAA,
  ELB_INDEX,
  KV_INDEX;

  /**
   * judge the index type.
   *
   * @param i an integer used to determine index type
   * @return index type
   */
  public static IndexType deserialize(short i) {
    switch (i) {
      case 0:
        return NO_INDEX;
      case 1:
        return RTREE_PAA;
      case 2:
        return ELB_INDEX;
      case 3:
        return KV_INDEX;
      default:
        throw new NotImplementedException("Given index is not implemented");
    }
  }

  public static int getSerializedSize() {
    return Short.BYTES;
  }

  /**
   * judge the index deserialize type.
   *
   * @return the integer used to determine index type
   */
  public short serialize() {
    switch (this) {
      case NO_INDEX:
        return 0;
      case RTREE_PAA:
        return 1;
      case ELB_INDEX:
        return 2;
      case KV_INDEX:
        return 3;
      default:
        throw new NotImplementedException("Given index is not implemented");
    }
  }

  public static IndexType getIndexType(String indexTypeString)
      throws UnsupportedIndexTypeException {
    String normalized = indexTypeString.toUpperCase();
    try {
      return IndexType.valueOf(normalized);
    } catch (IllegalArgumentException e) {
      throw new UnsupportedIndexTypeException(indexTypeString);
    }
  }
}
