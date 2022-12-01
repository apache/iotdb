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
package org.apache.iotdb.backup.core.model;

import org.apache.iotdb.tsfile.exception.NullFieldException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.utils.Binary;

/** @Author: LL @Description: @Date: create in 2022/7/19 10:33 */
public class FieldCopy {

  private TSDataType dataType;
  private boolean boolV;
  private int intV;
  private long longV;
  private float floatV;
  private double doubleV;
  private Binary binaryV;

  public FieldCopy(TSDataType dataType) {
    this.dataType = dataType;
  }

  public static FieldCopy copy(Field field) {
    if (field == null) {
      return null;
    }
    FieldCopy out = new FieldCopy(field.getDataType());
    if (out.dataType != null) {
      switch (out.dataType) {
        case DOUBLE:
          out.setDoubleV(field.getDoubleV());
          break;
        case FLOAT:
          out.setFloatV(field.getFloatV());
          break;
        case INT64:
          out.setLongV(field.getLongV());
          break;
        case INT32:
          out.setIntV(field.getIntV());
          break;
        case BOOLEAN:
          out.setBoolV(field.getBoolV());
          break;
        case TEXT:
          out.setBinaryV(field.getBinaryV());
          break;
        default:
          throw new UnSupportedDataTypeException(out.dataType.toString());
      }
    }

    return out;
  }

  public TSDataType getDataType() {
    return this.dataType;
  }

  public boolean getBoolV() {
    if (this.dataType == null) {
      throw new NullFieldException();
    } else {
      return this.boolV;
    }
  }

  public void setBoolV(boolean boolV) {
    this.boolV = boolV;
  }

  public int getIntV() {
    if (this.dataType == null) {
      throw new NullFieldException();
    } else {
      return this.intV;
    }
  }

  public void setIntV(int intV) {
    this.intV = intV;
  }

  public long getLongV() {
    if (this.dataType == null) {
      throw new NullFieldException();
    } else {
      return this.longV;
    }
  }

  public void setLongV(long longV) {
    this.longV = longV;
  }

  public float getFloatV() {
    if (this.dataType == null) {
      throw new NullFieldException();
    } else {
      return this.floatV;
    }
  }

  public void setFloatV(float floatV) {
    this.floatV = floatV;
  }

  public double getDoubleV() {
    if (this.dataType == null) {
      throw new NullFieldException();
    } else {
      return this.doubleV;
    }
  }

  public void setDoubleV(double doubleV) {
    this.doubleV = doubleV;
  }

  public Binary getBinaryV() {
    if (this.dataType == null) {
      throw new NullFieldException();
    } else {
      return this.binaryV;
    }
  }

  public void setBinaryV(Binary binaryV) {
    this.binaryV = binaryV;
  }

  public String getStringValue() {
    if (this.dataType == null) {
      return "null";
    } else {
      switch (this.dataType) {
        case DOUBLE:
          return String.valueOf(this.doubleV);
        case FLOAT:
          return String.valueOf(this.floatV);
        case INT64:
          return String.valueOf(this.longV);
        case INT32:
          return String.valueOf(this.intV);
        case BOOLEAN:
          return String.valueOf(this.boolV);
        case TEXT:
          return this.binaryV.toString();
        default:
          throw new UnSupportedDataTypeException(this.dataType.toString());
      }
    }
  }

  @Override
  public String toString() {
    return this.getStringValue();
  }

  public Object getObjectValue(TSDataType dataType) {
    if (this.dataType == null) {
      return null;
    } else {
      switch (dataType) {
        case DOUBLE:
          return this.getDoubleV();
        case FLOAT:
          return this.getFloatV();
        case INT64:
          return this.getLongV();
        case INT32:
          return this.getIntV();
        case BOOLEAN:
          return this.getBoolV();
        case TEXT:
          return this.getBinaryV();
        default:
          throw new UnSupportedDataTypeException(dataType.toString());
      }
    }
  }
}
