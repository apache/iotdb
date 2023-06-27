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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.SprintzEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class SprintzDecoder extends Decoder {
  protected static final Logger logger = LoggerFactory.getLogger(SprintzEncoder.class);
  protected int bitWidth;
  protected int Block_size = 8;
  protected boolean isBlockReaded;
  protected int currentCount;
  protected int decodeSize;

  public SprintzDecoder() {
    super(TSEncoding.SPRINTZ);
    isBlockReaded = false;
    currentCount = 0;
  }

  @Override
  public void reset() {
    isBlockReaded = false;
    currentCount = 0;
  }

  protected abstract void decodeBlock(ByteBuffer in) throws IOException;

  protected abstract void recalculate();
}
