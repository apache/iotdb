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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.unary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class InTransformer extends UnaryTransformer {

  private final Satisfy satisfy;

  private Set<Integer> intSet;
  private Set<Long> longSet;
  private Set<Float> floatSet;
  private Set<Double> doubleSet;
  private Set<Boolean> booleanSet;
  private Set<String> stringSet;

  public InTransformer(LayerPointReader layerPointReader, boolean isNotIn, Set<String> values) {
    super(layerPointReader);
    satisfy = isNotIn ? new NotInSatisfy() : new InSatisfy();
    initTypedSet(values);
  }

  private void initTypedSet(Set<String> values) {
    switch (layerPointReaderDataType) {
      case INT32:
        intSet = new HashSet<>();
        for (String value : values) {
          intSet.add(Integer.valueOf(value));
        }
        break;
      case INT64:
        longSet = new HashSet<>();
        for (String value : values) {
          longSet.add(Long.valueOf(value));
        }
        break;
      case FLOAT:
        floatSet = new HashSet<>();
        for (String value : values) {
          floatSet.add(Float.valueOf(value));
        }
        break;
      case DOUBLE:
        doubleSet = new HashSet<>();
        for (String value : values) {
          doubleSet.add(Double.valueOf(value));
        }
        break;
      case BOOLEAN:
        booleanSet = new HashSet<>();
        for (String value : values) {
          booleanSet.add(Boolean.valueOf(value));
        }
        break;
      case TEXT:
        stringSet = values;
        break;
      default:
        throw new UnsupportedOperationException(
            "unsupported data type: " + layerPointReader.getDataType());
    }
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }

  @Override
  protected void transformAndCache() throws QueryProcessException, IOException {
    switch (layerPointReaderDataType) {
      case INT32:
        int intValue = layerPointReader.currentInt();
        cachedBoolean = satisfy.of(intValue);
        break;
      case INT64:
        long longValue = layerPointReader.currentLong();
        cachedBoolean = satisfy.of(longValue);
        break;
      case FLOAT:
        float floatValue = layerPointReader.currentFloat();
        cachedBoolean = satisfy.of(floatValue);
        break;
      case DOUBLE:
        double doubleValue = layerPointReader.currentDouble();
        cachedBoolean = satisfy.of(doubleValue);
        break;
      case BOOLEAN:
        boolean booleanValue = layerPointReader.currentBoolean();
        cachedBoolean = satisfy.of(booleanValue);
        break;
      case TEXT:
        Binary binaryValue = layerPointReader.currentBinary();
        cachedBoolean = satisfy.of(binaryValue.getStringValue());
        break;
      default:
        throw new QueryProcessException("unsupported data type: " + layerPointReader.getDataType());
    }
  }

  private interface Satisfy {

    boolean of(int intValue);

    boolean of(long longValue);

    boolean of(float floatValue);

    boolean of(double doubleValue);

    boolean of(boolean booleanValue);

    boolean of(String stringValue);
  }

  private class InSatisfy implements Satisfy {

    @Override
    public boolean of(int intValue) {
      return intSet.contains(intValue);
    }

    @Override
    public boolean of(long longValue) {
      return longSet.contains(longValue);
    }

    @Override
    public boolean of(float floatValue) {
      return floatSet.contains(floatValue);
    }

    @Override
    public boolean of(double doubleValue) {
      return doubleSet.contains(doubleValue);
    }

    @Override
    public boolean of(boolean booleanValue) {
      return booleanSet.contains(booleanValue);
    }

    @Override
    public boolean of(String stringValue) {
      return stringSet.contains(stringValue);
    }
  }

  private class NotInSatisfy implements Satisfy {

    @Override
    public boolean of(int intValue) {
      return !intSet.contains(intValue);
    }

    @Override
    public boolean of(long longValue) {
      return !longSet.contains(longValue);
    }

    @Override
    public boolean of(float floatValue) {
      return !floatSet.contains(floatValue);
    }

    @Override
    public boolean of(double doubleValue) {
      return !doubleSet.contains(doubleValue);
    }

    @Override
    public boolean of(boolean booleanValue) {
      return !booleanSet.contains(booleanValue);
    }

    @Override
    public boolean of(String stringValue) {
      return !stringSet.contains(stringValue);
    }
  }
}
