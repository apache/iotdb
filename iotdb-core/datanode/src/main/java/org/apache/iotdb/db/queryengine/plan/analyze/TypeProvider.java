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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.utils.TypeUtil;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;

public class TypeProvider {

  private final Map<String, TSDataType> treeModelTypeMap;

  private TemplatedInfo templatedInfo;

  public TypeProvider() {
    this.treeModelTypeMap = new HashMap<>();
  }

  public TypeProvider(Map<String, TSDataType> treeModelTypeMap, TemplatedInfo templatedInfo) {
    this.treeModelTypeMap = treeModelTypeMap;
    this.templatedInfo = templatedInfo;
    // The type of TimeStampOperand is INT64
    this.treeModelTypeMap.putIfAbsent(TIMESTAMP_EXPRESSION_STRING, TSDataType.INT64);
    this.tableModelTypes = null;
  }

  public TSDataType getTreeModelType(String symbol) {
    return treeModelTypeMap.get(symbol);
  }

  public void setTreeModelType(String symbol, TSDataType dataType) {
    // DataType of NullOperand is null, we needn't put it into TypeProvider
    if (dataType != null) {
      this.treeModelTypeMap.put(symbol, dataType);
    }
  }

  public Map<String, TSDataType> getTreeModelTypeMap() {
    return this.treeModelTypeMap;
  }

  public void setTemplatedInfo(TemplatedInfo templatedInfo) {
    this.templatedInfo = templatedInfo;
  }

  public TemplatedInfo getTemplatedInfo() {
    return this.templatedInfo;
  }

  // ----------------------used for relational model----------------------------

  private Map<Symbol, Type> tableModelTypes = new HashMap<>();

  public static TypeProvider viewOf(Map<Symbol, Type> types) {
    return new TypeProvider(types);
  }

  public static TypeProvider copyOf(Map<Symbol, Type> types) {
    return new TypeProvider(ImmutableMap.copyOf(types));
  }

  public static TypeProvider empty() {
    return new TypeProvider(ImmutableMap.of());
  }

  public TypeProvider(Map<Symbol, Type> tableModelTypes) {
    this.tableModelTypes = tableModelTypes;

    this.treeModelTypeMap = null;
  }

  public TypeProvider(
      Map<String, TSDataType> treeModelTypeMap,
      TemplatedInfo templatedInfo,
      Map<Symbol, Type> tableModelTypes) {
    this.treeModelTypeMap = treeModelTypeMap;
    this.templatedInfo = templatedInfo;

    this.tableModelTypes = tableModelTypes;
  }

  public Type getTableModelType(Symbol symbol) {
    requireNonNull(symbol, "symbol is null");

    Type type = tableModelTypes.get(symbol);
    checkArgument(type != null, "no type found for symbol '%s' in TypeProvider", symbol);

    return type;
  }

  public boolean isSymbolExist(Symbol symbol) {
    return tableModelTypes.containsKey(symbol);
  }

  public void putTableModelType(Symbol symbol, Type type) {
    requireNonNull(symbol, "symbol is null");

    tableModelTypes.put(symbol, type);
  }

  public Map<Symbol, Type> allTableModelTypes() {
    // types may be a HashMap, so creating an ImmutableMap here would add extra cost when allTypes
    // gets called frequently
    return Collections.unmodifiableMap(tableModelTypes);
  }

  public void setTableModelTypes(Map<Symbol, Type> tableModelTypes) {
    this.tableModelTypes = tableModelTypes;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(treeModelTypeMap.size(), byteBuffer);
    for (Map.Entry<String, TSDataType> entry : treeModelTypeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().ordinal(), byteBuffer);
    }

    if (templatedInfo == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      templatedInfo.serialize(byteBuffer);
    }

    if (tableModelTypes == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write(tableModelTypes.size(), byteBuffer);
      for (Map.Entry<Symbol, Type> entry : tableModelTypes.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey().getName(), byteBuffer);
        TypeUtil.serialize(entry.getValue(), byteBuffer);
      }
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(treeModelTypeMap == null ? 0 : treeModelTypeMap.size(), stream);
    if (treeModelTypeMap != null) {
      for (Map.Entry<String, TSDataType> entry : treeModelTypeMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue().ordinal(), stream);
      }
    }

    if (templatedInfo == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      templatedInfo.serialize(stream);
    }

    if (tableModelTypes == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(tableModelTypes.size(), stream);
      for (Map.Entry<Symbol, Type> entry : tableModelTypes.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey().getName(), stream);
        TypeUtil.serialize(entry.getValue(), stream);
      }
    }
  }

  public static TypeProvider deserialize(ByteBuffer byteBuffer) {
    int mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, TSDataType> typeMap = new HashMap<>(mapSize);
    while (mapSize > 0) {
      typeMap.put(
          ReadWriteIOUtils.readString(byteBuffer),
          TSDataType.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
      mapSize--;
    }

    TemplatedInfo templatedInfo = null;
    byte hasTemplatedInfo = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasTemplatedInfo == 1) {
      templatedInfo = TemplatedInfo.deserialize(byteBuffer);
    }

    Map<Symbol, Type> tableModelTypes = null;
    byte hasTableModelTypes = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasTableModelTypes == 1) {
      mapSize = ReadWriteIOUtils.readInt(byteBuffer);
      tableModelTypes = new HashMap<>(mapSize);
      while (mapSize > 0) {
        tableModelTypes.put(
            new Symbol(ReadWriteIOUtils.readString(byteBuffer)), TypeUtil.deserialize(byteBuffer));
        mapSize--;
      }
    }

    return new TypeProvider(typeMap, templatedInfo, tableModelTypes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypeProvider that = (TypeProvider) o;
    return Objects.equals(treeModelTypeMap, that.treeModelTypeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(treeModelTypeMap);
  }
}
