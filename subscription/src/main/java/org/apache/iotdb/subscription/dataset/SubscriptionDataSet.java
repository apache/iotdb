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

package org.apache.iotdb.subscription.dataset;

import org.apache.iotdb.subscription.api.dataset.ISubscriptionDataSet;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

public class SubscriptionDataSet implements ISubscriptionDataSet {
  private Long time;
  private List<String> columnNames;
  private List<String> columnTypes;
  private List<Object> dataResult;
  private IDataGetter dataGetter;

  public Long getTime() {
    return time;
  }

  public void setTime(Long time) {
    this.time = time;
  }

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  @Override
  public List<String> getColumnNames() {
    return this.columnNames;
  }

  public void setColumnTypes(List<String> columnTypes) {
    this.columnTypes = columnTypes;
  }

  @Override
  public List<String> getColumnTypes() {
    return columnTypes;
  }

  public void setDataResult(List<Object> dataResult) {
    this.dataResult = dataResult;
  }

  @Override
  public List<Object> getDataResult() {
    return this.dataResult;
  }

  @Override
  public IDataGetter Data() {
    if (this.dataGetter == null) {
      this.dataGetter = new DataGetter();
    }
    return this.dataGetter;
  }

  public class DataGetter implements IDataGetter {
    public boolean getBoolean(int columnIndex) {
      Optional<Object> column = Optional.ofNullable(getObject(columnIndex));
      if (column.isPresent()) {
        return (Boolean) column.get();
      }
      return false;
    }

    public boolean getBoolean(String columnName) {
      return getBoolean(findColumn(columnName));
    }

    public double getDouble(int columnIndex) {
      Optional<Object> column = Optional.ofNullable(getObject(columnIndex));
      if (column.isPresent()) {
        return (Double) column.get();
      }
      return 0;
    }

    public double getDouble(String columnName) {
      return getDouble(findColumn(columnName));
    }

    public float getFloat(int columnIndex) {
      Optional<Object> column = Optional.ofNullable(getObject(columnIndex));
      if (column.isPresent()) {
        return (Float) column.get();
      }
      return 0;
    }

    public float getFloat(String columnName) {
      return getFloat(findColumn(columnName));
    }

    public int getInt(int columnIndex) {
      Optional<Object> column = Optional.ofNullable(getObject(columnIndex));
      if (column.isPresent()) {
        return (Integer) column.get();
      }
      return 0;
    }

    public int getInt(String columnName) {
      return getInt(findColumn(columnName));
    }

    public long getLong(int columnIndex) {
      Optional<Object> column = Optional.ofNullable(getObject(columnIndex));
      if (column.isPresent()) {
        return (Long) column.get();
      }
      return 0;
    }

    public long getLong(String columnName) {
      return getLong(findColumn(columnName));
    }

    public Object getObject(int columnIndex) {
      return dataResult.get(columnIndex);
    }

    public Object getObject(String columnName) {
      return getObject(findColumn(columnName));
    }

    public String getString(int columnIndex) {
      Optional<Object> column = Optional.ofNullable(getObject(columnIndex));
      if (column.isPresent()) {
        return column.toString();
      }
      return null;
    }

    public String getString(String columnName) {
      return getString(findColumn(columnName));
    }

    public Timestamp getTimestamp(int columnIndex) {
      return new Timestamp(getLong(columnIndex));
    }

    public Timestamp getTimestamp(String columnName) {
      return getTimestamp(findColumn(columnName));
    }

    public int findColumn(String columnName) {
      return columnNames.indexOf(columnName);
    }
  }
}
