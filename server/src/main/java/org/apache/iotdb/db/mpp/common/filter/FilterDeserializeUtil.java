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
package org.apache.iotdb.db.mpp.common.filter;

import java.nio.ByteBuffer;

public class FilterDeserializeUtil {

  public static QueryFilter deserialize(ByteBuffer buffer) {
    byte filterType = buffer.get();
    switch (filterType) {
      case 0:
        return QueryFilter.deserialize(buffer);
      case 1:
        return FunctionFilter.deserialize(buffer);
      case 2:
        return BasicFunctionFilter.deserialize(buffer);
      case 3:
        return InFilter.deserialize(buffer);
      case 4:
        return LikeFilter.deserialize(buffer);
      case 5:
        return RegexpFilter.deserialize(buffer);
      default:
        throw new IllegalArgumentException("Invalid filter type: " + filterType);
    }
  }
}

enum FilterTypes {
  Query((byte) 0),
  Function((byte) 1),
  BasicFunction((byte) 2),
  In((byte) 3),
  Like((byte) 4),
  Regexp((byte) 5);

  private final byte filterType;

  FilterTypes(byte filterType) {
    this.filterType = filterType;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.put(filterType);
  }
}
