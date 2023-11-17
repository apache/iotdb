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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.Serializable;
import java.util.Arrays;

public abstract class TsPrimitiveType implements Serializable {

  /**
   * get tsPrimitiveType by resultDataType.
   *
   * @param dataType given TsDataType
   */
  public static TsPrimitiveType getByType(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return new TsPrimitiveType.TsBoolean();
      case INT32:
        return new TsPrimitiveType.TsInt();
      case INT64:
        return new TsPrimitiveType.TsLong();
      case FLOAT:
        return new TsPrimitiveType.TsFloat();
      case DOUBLE:
        return new TsPrimitiveType.TsDouble();
      case TEXT:
        return new TsPrimitiveType.TsBinary();
      case VECTOR:
        return new TsPrimitiveType.TsVector();
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  /**
   * get tsPrimitiveType by resultDataType and initial value.
   *
   * @param dataType given TsDataType
   * @param v initial value
   */
  public static TsPrimitiveType getByType(TSDataType dataType, Object v) {
    switch (dataType) {
      case BOOLEAN:
        return new TsPrimitiveType.TsBoolean((boolean) v);
      case INT32:
        return new TsPrimitiveType.TsInt((int) v);
      case INT64:
        return new TsPrimitiveType.TsLong((long) v);
      case FLOAT:
        return new TsPrimitiveType.TsFloat((float) v);
      case DOUBLE:
        return new TsPrimitiveType.TsDouble((double) v);
      case TEXT:
        return new TsPrimitiveType.TsBinary((Binary) v);
      case VECTOR:
        return new TsPrimitiveType.TsVector((TsPrimitiveType[]) v);
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  public boolean getBoolean() {
    throw new UnsupportedOperationException("getBoolean() is not supported for current sub-class");
  }

  public int getInt() {
    throw new UnsupportedOperationException("getInt() is not supported for current sub-class");
  }

  public long getLong() {
    throw new UnsupportedOperationException("getLong() is not supported for current sub-class");
  }

  public float getFloat() {
    throw new UnsupportedOperationException("getFloat() is not supported for current sub-class");
  }

  public double getDouble() {
    throw new UnsupportedOperationException("getDouble() is not supported for current sub-class");
  }

  public Binary getBinary() {
    throw new UnsupportedOperationException("getBinary() is not supported for current sub-class");
  }

  public TsPrimitiveType[] getVector() {
    throw new UnsupportedOperationException("getVector() is not supported for current sub-class");
  }

  public void setBoolean(boolean val) {
    throw new UnsupportedOperationException("setBoolean() is not supported for current sub-class");
  }

  public void setInt(int val) {
    throw new UnsupportedOperationException("setInt() is not supported for current sub-class");
  }

  public void setLong(long val) {
    throw new UnsupportedOperationException("setLong() is not supported for current sub-class");
  }

  public void setFloat(float val) {
    throw new UnsupportedOperationException("setFloat() is not supported for current sub-class");
  }

  public void setDouble(double val) {
    throw new UnsupportedOperationException("setDouble() is not supported for current sub-class");
  }

  public void setBinary(Binary val) {
    throw new UnsupportedOperationException("setBinary() is not supported for current sub-class");
  }

  public void setVector(TsPrimitiveType[] val) {
    throw new UnsupportedOperationException("setVector() is not supported for current sub-class");
  }

  public abstract void setObject(Object val);

  public abstract void reset();

  /**
   * get the size of one instance of current class.
   *
   * @return size of one instance of current class
   */
  public abstract int getSize();

  public abstract Object getValue();

  public abstract String getStringValue();

  public abstract TSDataType getDataType();

  @Override
  public String toString() {
    return getStringValue();
  }

  @Override
  public boolean equals(Object object) {
    return (object instanceof TsPrimitiveType)
        && (((TsPrimitiveType) object).getValue().equals(getValue()));
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  public static class TsBoolean extends TsPrimitiveType {

    private boolean value;

    public TsBoolean() {}

    public TsBoolean(boolean value) {
      this.value = value;
    }

    @Override
    public boolean getBoolean() {
      return value;
    }

    @Override
    public void setBoolean(boolean val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Boolean) {
        setBoolean((Boolean) val);
        return;
      }
      throw new UnSupportedDataTypeException("TsBoolean can only be set Boolean value");
    }

    @Override
    public void reset() {
      value = false;
    }

    @Override
    public int getSize() {
      return 4 + 1;
    }

    @Override
    public Object getValue() {
      return getBoolean();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.BOOLEAN;
    }

    @Override
    public int hashCode() {
      return Boolean.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsBoolean) {
        TsBoolean anotherTs = (TsBoolean) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsInt extends TsPrimitiveType {

    private int value;

    public TsInt() {}

    public TsInt(int value) {
      this.value = value;
    }

    @Override
    public int getInt() {
      return value;
    }

    @Override
    public void setInt(int val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Integer) {
        setInt((Integer) val);
        return;
      }
      throw new UnSupportedDataTypeException("TsInt can only be set Integer value");
    }

    @Override
    public void reset() {
      value = 0;
    }

    @Override
    public int getSize() {
      return 4 + 4;
    }

    @Override
    public Object getValue() {
      return getInt();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.INT32;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsInt) {
        TsInt anotherTs = (TsInt) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsLong extends TsPrimitiveType {

    private long value;

    public TsLong() {}

    public TsLong(long value) {
      this.value = value;
    }

    @Override
    public long getLong() {
      return value;
    }

    @Override
    public void setLong(long val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Long) {
        setLong((Long) val);
        return;
      }
      throw new UnSupportedDataTypeException("TsLong can only be set Long value");
    }

    @Override
    public void reset() {
      value = 0;
    }

    @Override
    public int getSize() {
      return 4 + 8;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.INT64;
    }

    @Override
    public Object getValue() {
      return getLong();
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsLong) {
        TsLong anotherTs = (TsLong) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsFloat extends TsPrimitiveType {

    private float value;

    public TsFloat() {}

    public TsFloat(float value) {
      this.value = value;
    }

    @Override
    public float getFloat() {
      return value;
    }

    @Override
    public void setFloat(float val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Float) {
        setFloat((Float) val);
        return;
      }
      throw new UnSupportedDataTypeException("TsFloat can only be set float value");
    }

    @Override
    public void reset() {
      value = 0;
    }

    @Override
    public int getSize() {
      return 4 + 4;
    }

    @Override
    public Object getValue() {
      return getFloat();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.FLOAT;
    }

    @Override
    public int hashCode() {
      return Float.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsFloat) {
        TsFloat anotherTs = (TsFloat) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsDouble extends TsPrimitiveType {

    private double value;

    public TsDouble() {}

    public TsDouble(double value) {
      this.value = value;
    }

    @Override
    public double getDouble() {
      return value;
    }

    @Override
    public void setDouble(double val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Double) {
        setDouble((Double) val);
        return;
      }
      throw new UnSupportedDataTypeException("TsDouble can only be set Double value");
    }

    @Override
    public void reset() {
      value = 0.0;
    }

    @Override
    public int getSize() {
      return 4 + 8;
    }

    @Override
    public Object getValue() {
      return getDouble();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.DOUBLE;
    }

    @Override
    public int hashCode() {
      return Double.hashCode(value);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsDouble) {
        TsDouble anotherTs = (TsDouble) anObject;
        return value == anotherTs.value;
      }
      return false;
    }
  }

  public static class TsBinary extends TsPrimitiveType {

    private Binary value;

    public TsBinary() {}

    public TsBinary(Binary value) {
      this.value = value;
    }

    @Override
    public Binary getBinary() {
      return value;
    }

    @Override
    public void setBinary(Binary val) {
      this.value = val;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof Binary) {
        setBinary((Binary) val);
        return;
      }
      throw new UnSupportedDataTypeException("TsBinary can only be set Binary value");
    }

    @Override
    public void reset() {
      value = null;
    }

    @Override
    public int getSize() {
      return 4 + 4 + value.getLength();
    }

    @Override
    public Object getValue() {
      return getBinary();
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.TEXT;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsBinary) {
        TsBinary anotherTs = (TsBinary) anObject;
        return value.equals(anotherTs.value);
      }
      return false;
    }
  }

  public static class TsVector extends TsPrimitiveType {

    private TsPrimitiveType[] values;

    public TsVector() {}

    public TsVector(TsPrimitiveType[] values) {
      this.values = values;
    }

    @Override
    public TsPrimitiveType[] getVector() {
      return values;
    }

    @Override
    public void setVector(TsPrimitiveType[] vals) {
      this.values = vals;
    }

    @Override
    public void setObject(Object val) {
      if (val instanceof TsPrimitiveType[]) {
        setVector((TsPrimitiveType[]) val);
        return;
      }
      throw new UnSupportedDataTypeException("TsVector can only be set TsPrimitiveType[] value");
    }

    @Override
    public void reset() {
      values = null;
    }

    @Override
    public int getSize() {
      int size = 0;
      for (TsPrimitiveType type : values) {
        if (type != null) {
          size += type.getSize();
        }
      }
      // object header + array object header
      return 4 + 4 + size;
    }

    @Override
    public Object getValue() {
      return getVector();
    }

    @Override
    public String getStringValue() {
      StringBuilder builder = new StringBuilder("[");
      builder.append(values[0] == null ? "null" : values[0].getStringValue());
      for (int i = 1; i < values.length; i++) {
        builder.append(", ").append(values[i] == null ? "null" : values[i].getStringValue());
      }
      builder.append("]");
      return builder.toString();
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.VECTOR;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof TsVector) {
        TsVector anotherTs = (TsVector) anObject;
        if (anotherTs.values.length != this.values.length) {
          return false;
        }
        for (int i = 0; i < this.values.length; i++) {
          if (!values[i].equals(anotherTs.values[i])) {
            return false;
          }
        }
        return true;
      }
      return false;
    }
  }
}
