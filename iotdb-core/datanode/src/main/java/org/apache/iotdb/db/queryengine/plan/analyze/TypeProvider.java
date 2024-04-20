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

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.TypeFactory;
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

  private final Map<String, TSDataType> typeMap;

  private TemplatedInfo templatedInfo;

  public TypeProvider() {
    this.typeMap = new HashMap<>();
    types = null;
  }

  public TypeProvider(Map<String, TSDataType> typeMap, TemplatedInfo templatedInfo) {
    this.typeMap = typeMap;
    this.templatedInfo = templatedInfo;
    // The type of TimeStampOperand is INT64
    this.typeMap.putIfAbsent(TIMESTAMP_EXPRESSION_STRING, TSDataType.INT64);
    types = null;
  }

  public TSDataType getType(String symbol) {
    return typeMap.get(symbol);
  }

  public void setType(String symbol, TSDataType dataType) {
    // DataType of NullOperand is null, we needn't put it into TypeProvider
    if (dataType != null) {
      this.typeMap.put(symbol, dataType);
    }
  }

  public Map<String, TSDataType> getTypeMap() {
    return this.typeMap;
  }

  public void setTemplatedInfo(TemplatedInfo templatedInfo) {
    this.templatedInfo = templatedInfo;
  }

  public TemplatedInfo getTemplatedInfo() {
    return this.templatedInfo;
  }

  // ----------------------used for relational model----------------------------

  private final Map<Symbol, Type> types;

  public static TypeProvider viewOf(Map<Symbol, Type> types) {
    return new TypeProvider(types);
  }

  public static TypeProvider copyOf(Map<Symbol, Type> types) {
    return new TypeProvider(ImmutableMap.copyOf(types));
  }

  public static TypeProvider empty() {
    return new TypeProvider(ImmutableMap.of());
  }

  public TypeProvider(Map<Symbol, Type> types) {
    this.types = types;

    this.typeMap = null;
  }

  public TypeProvider(
      Map<String, TSDataType> typeMap, TemplatedInfo templatedInfo, Map<Symbol, Type> types) {
    this.typeMap = typeMap;
    this.templatedInfo = templatedInfo;
    this.types = types;
  }

  public Type getTableModelType(Symbol symbol) {
    requireNonNull(symbol, "symbol is null");

    Type type = types.get(symbol);
    checkArgument(type != null, "no type found for symbol '%s'", symbol);

    return type;
  }

  public void putTableModelType(Symbol symbol, Type type) {
    requireNonNull(symbol, "symbol is null");

    types.put(symbol, type);
  }

  public Map<Symbol, Type> allTypes() {
    // types may be a HashMap, so creating an ImmutableMap here would add extra cost when allTypes
    // gets called frequently
    return Collections.unmodifiableMap(types);
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(typeMap.size(), byteBuffer);
    for (Map.Entry<String, TSDataType> entry : typeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().ordinal(), byteBuffer);
    }

    if (templatedInfo == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      templatedInfo.serialize(byteBuffer);
    }

    if (types == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write(types.size(), byteBuffer);
      for (Map.Entry<Symbol, Type> entry : types.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey().getName(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue().getTypeEnum().ordinal(), byteBuffer);
      }
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(typeMap.size(), stream);
    for (Map.Entry<String, TSDataType> entry : typeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue().ordinal(), stream);
    }

    if (templatedInfo == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      templatedInfo.serialize(stream);
    }

    if (types == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write(types.size(), stream);
      for (Map.Entry<Symbol, Type> entry : types.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey().getName(), stream);
        ReadWriteIOUtils.write(entry.getValue().getTypeEnum().ordinal(), stream);
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

    Map<Symbol, Type> types = null;
    byte hasTypes = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasTypes == 1) {
      mapSize = ReadWriteIOUtils.readInt(byteBuffer);
      types = new HashMap<>(mapSize);
      while (mapSize > 0) {
        types.put(
            new Symbol(ReadWriteIOUtils.readString(byteBuffer)),
            TypeFactory.getType(TypeEnum.values()[ReadWriteIOUtils.readInt(byteBuffer)]));
        mapSize--;
      }
    }

    return new TypeProvider(typeMap, templatedInfo, types);
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
    return Objects.equals(typeMap, that.typeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeMap);
  }
}
