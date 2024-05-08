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

import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_DOUBLE;

/**
 * This class includes code modified from Michael Burman's gorilla-tsc project.
 *
 * <p>Copyright: 2016-2018 Michael Burman and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class DoublePrecisionDecoderV2 extends LongGorillaDecoder {

  private static final long GORILLA_ENCODING_ENDING =
      Double.doubleToRawLongBits(GORILLA_ENCODING_ENDING_DOUBLE);

  @Override
  public final double readDouble(ByteBuffer in) {
    return Double.longBitsToDouble(readLong(in));
  }

  @Override
  protected long cacheNext(ByteBuffer in) {
    readNext(in);
    if (storedValue == GORILLA_ENCODING_ENDING) {
      hasNext = false;
    }
    return storedValue;
  }
}
