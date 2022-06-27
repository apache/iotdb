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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.component.FilterNullPolicy;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FilterNullParameter {

  // The policy to discard the result from upstream npde
  private FilterNullPolicy filterNullPolicy;

  // indicate columns used to filter null
  private List<Expression> filterNullColumns;

  public FilterNullParameter() {}

  public FilterNullParameter(
      FilterNullPolicy filterNullPolicy, List<Expression> filterNullColumns) {
    this.filterNullPolicy = filterNullPolicy;
    this.filterNullColumns = filterNullColumns;
  }

  public FilterNullPolicy getFilterNullPolicy() {
    return filterNullPolicy;
  }

  public List<Expression> getFilterNullColumns() {
    return filterNullColumns;
  }

  public void setFilterNullPolicy(FilterNullPolicy filterNullPolicy) {
    this.filterNullPolicy = filterNullPolicy;
  }

  public void setFilterNullColumns(List<Expression> filterNullColumns) {
    this.filterNullColumns = filterNullColumns;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(filterNullPolicy.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(filterNullColumns.size(), byteBuffer);
    for (Expression filterNullColumn : filterNullColumns) {
      Expression.serialize(filterNullColumn, byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(filterNullPolicy.ordinal(), stream);
    ReadWriteIOUtils.write(filterNullColumns.size(), stream);
    for (Expression filterNullColumn : filterNullColumns) {
      Expression.serialize(filterNullColumn, stream);
    }
  }

  public static FilterNullParameter deserialize(ByteBuffer byteBuffer) {
    FilterNullPolicy filterNullPolicy =
        FilterNullPolicy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Expression> filterNullColumns = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      filterNullColumns.add(Expression.deserialize(byteBuffer));
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
