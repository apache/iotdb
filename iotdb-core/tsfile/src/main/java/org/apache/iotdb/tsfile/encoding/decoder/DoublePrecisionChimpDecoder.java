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

/**
 * This class includes code modified from Panagiotis Liakos chimp project.
 *
 * <p>Copyright: 2022- Panagiotis Liakos, Katia Papakonstantinopoulou and Yannis Kotidis
 *
 * <p>Project page: https://github.com/panagiotisl/chimp
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class DoublePrecisionChimpDecoder extends LongChimpDecoder {

  private static final long CHIMP_ENCODING_ENDING = Double.doubleToRawLongBits(Double.NaN);

  @Override
  public final double readDouble(ByteBuffer in) {
    return Double.longBitsToDouble(readLong(in));
  }

  @Override
  protected long cacheNext(ByteBuffer in) {
    readNext(in);
    if (storedValues[current] == CHIMP_ENCODING_ENDING) {
      hasNext = false;
    }
    return storedValues[current];
  }
}
