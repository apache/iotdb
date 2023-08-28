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
package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * This class is the parent class of all Encoders. Every encoder has a specific {@code
 * <encoderType>} which represents the type of this encoder
 */
public abstract class Encoder {

  public static final String MAX_STRING_LENGTH = "max_string_length";
  public static final String MAX_POINT_NUMBER = "max_point_number";

  private TSEncoding type;

  public void setType(TSEncoding type) {
    this.type = type;
  }

  public TSEncoding getType() {
    return type;
  }

  public Encoder(TSEncoding type) {
    this.type = type;
  }

  public void encode(boolean value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode boolean is not supported by Encoder");
  }

  public void encode(short value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode short is not supported by Encoder");
  }

  public void encode(int value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode int is not supported by Encoder");
  }

  public void encode(long value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode long is not supported by Encoder");
  }

  public void encode(float value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode float is not supported by Encoder");
  }

  public void encode(double value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode double is not supported by Encoder");
  }

  public void encode(Binary value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode Binary is not supported by Encoder");
  }

  public void encode(BigDecimal value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException("Method encode BigDecimal is not supported by Encoder");
  }

  /**
   * Write all values buffered in memory cache to OutputStream.
   *
   * @param out - ByteArrayOutputStream
   * @throws IOException cannot flush to OutputStream
   */
  public abstract void flush(ByteArrayOutputStream out) throws IOException;

  /**
   * When encoder accepts a new incoming data point, the maximal possible size in byte it takes to
   * store in memory.
   *
   * @return the maximal possible size of one data item encoded by this encoder
   */
  public int getOneItemMaxSize() {
    throw new UnsupportedOperationException();
  }

  /**
   * The maximal possible memory size occupied by current Encoder. This statistic value doesn't
   * involve OutputStream.
   *
   * @return the maximal size of possible memory occupied by current encoder
   */
  public long getMaxByteSize() {
    throw new UnsupportedOperationException();
  }
}
