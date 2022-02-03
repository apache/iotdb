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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class SprintzEncoder extends Encoder {
  protected static final Logger logger = LoggerFactory.getLogger(SprintzEncoder.class);

  /** Segment block size to compress:8 */
  protected int Block_size = 8;

  /** group size maximum * */
  protected int groupMax = 16;

  /** group number * */
  protected int groupNum;

  /** the bit width used for bit-packing and rle. */
  protected int bitWidth;

  /** output stream to buffer {@code <bitwidth> <encoded-data>}. */
  protected ByteArrayOutputStream byteCache;

  /** selecet the predict method */
  protected String PredictMethod =
      TSFileDescriptor.getInstance().getConfig().getSprintzPredictScheme();;

  protected boolean isFirstCached = false;

  protected TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  public SprintzEncoder() {
    super(TSEncoding.SPRINTZ);
    byteCache = new ByteArrayOutputStream();
  }

  protected void reset() {
    byteCache.reset();
    isFirstCached = false;
    groupNum = 0;
  }

  protected abstract void bitPack() throws IOException;

  protected abstract void entropy();
}
