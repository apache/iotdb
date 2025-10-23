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

package org.apache.iotdb.commons.schema.view.viewExpression.leaf;

import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ConstantViewOperand extends LeafViewOperand {

  // region member variables and init functions
  private final String valueString;
  private final TSDataType dataType;

  public ConstantViewOperand(TSDataType dataType, String valueString) {
    this.dataType = Validate.notNull(dataType);
    this.valueString = Validate.notNull(valueString);
  }

  public ConstantViewOperand(ByteBuffer byteBuffer) {
    dataType = TSDataType.deserializeFrom(byteBuffer);
    valueString = ReadWriteIOUtils.readString(byteBuffer);
  }

  public ConstantViewOperand(InputStream inputStream) {
    try {
      dataType = TSDataType.deserialize(ReadWriteIOUtils.readByte(inputStream));
      valueString = ReadWriteIOUtils.readString(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // endregion

  // region common interfaces that have to be implemented
  @Override
  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitConstantOperand(this, context);
  }

  @Override
  public ViewExpressionType getExpressionType() {
    return ViewExpressionType.CONSTANT;
  }

  @Override
  public String toString(boolean isRoot) {
    return this.valueString;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    dataType.serializeTo(byteBuffer);
    ReadWriteIOUtils.write(valueString, byteBuffer);
  }

  @Override
  protected void serialize(OutputStream stream) throws IOException {
    stream.write(dataType.serialize());
    ReadWriteIOUtils.write(valueString, stream);
  }

  // endregion

  public TSDataType getDataType() {
    return dataType;
  }

  public String getValueString() {
    return valueString;
  }
}
