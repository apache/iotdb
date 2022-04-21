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

package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.db.mpp.sql.planner.plan.InputLocation;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FilterNullParameter {

  // The policy to discard the result from upstream npde
  private final FilterNullPolicy filterNullPolicy;

  // indicate columns used to filter null
  private final List<InputLocation> filterNullColumns;

  public FilterNullParameter(
      FilterNullPolicy filterNullPolicy, List<InputLocation> filterNullColumns) {
    this.filterNullPolicy = filterNullPolicy;
    this.filterNullColumns = filterNullColumns;
  }

  public FilterNullPolicy getFilterNullPolicy() {
    return filterNullPolicy;
  }

  public List<InputLocation> getFilterNullColumns() {
    return filterNullColumns;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(filterNullPolicy.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(filterNullColumns.size(), byteBuffer);
    for (InputLocation filterNullColumn : filterNullColumns) {
      filterNullColumn.serialize(byteBuffer);
    }
  }

  public static FilterNullParameter deserialize(ByteBuffer byteBuffer) {
    FilterNullPolicy filterNullPolicy =
        FilterNullPolicy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<InputLocation> filterNullColumns = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      filterNullColumns.add(InputLocation.deserialize(byteBuffer));
    }
    return new FilterNullParameter(filterNullPolicy, filterNullColumns);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilterNullParameter that = (FilterNullParameter) o;
    return filterNullPolicy == that.filterNullPolicy
        && Objects.equals(filterNullColumns, that.filterNullColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filterNullPolicy, filterNullColumns);
  }
}
