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

package org.apache.iotdb.commons.exception.pipe;

import org.apache.iotdb.pipe.api.exception.PipeException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class PipeRuntimeException extends PipeException {

  protected PipeRuntimeException(final String message) {
    super(message);
  }

  protected PipeRuntimeException(final String message, final long timeStamp) {
    super(message, timeStamp);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof PipeRuntimeException
        && Objects.equals(getMessage(), ((PipeRuntimeException) obj).getMessage())
        && Objects.equals(getTimeStamp(), ((PipeRuntimeException) obj).getTimeStamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getTimeStamp());
  }

  public abstract void serialize(final ByteBuffer byteBuffer);

  public abstract void serialize(final OutputStream stream) throws IOException;
}
