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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

public class RLBE extends Encoder {
  /** Every BLOCK_DEFAULT_SIZE values are followed by a header */
  protected static final int BLOCK_DEFAULT_SIZE = 10000;

  protected static final Logger logger = LoggerFactory.getLogger(RLBE.class);

  /** output stream to buffer {@code <length> <fibonacci code> <delta value>} */
  protected ByteArrayOutputStream out;

  protected int blockSize = BLOCK_DEFAULT_SIZE;

  /** Storage bits into byteBuffer and flush when full */
  protected byte byteBuffer;
  /** Valid bits left in byteBuffer */
  protected int numberLeftInBuffer;

  /** Differential Value of InputData is stored in DiffValue */

  /** Length of binary code of delta values are stored in LengthCode */
  protected int[] LengthCode;

  /** When writeIndex == -1, the first value is not stored */
  protected int writeIndex = -1;

  /** Constructor of RLBE */
  public RLBE() {
    super(TSEncoding.RLBE);
    // blockSize = size;
  }

  @Override
  public void flush(ByteArrayOutputStream out) {}
}
