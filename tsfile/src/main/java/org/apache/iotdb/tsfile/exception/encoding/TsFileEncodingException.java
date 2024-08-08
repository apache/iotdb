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

package org.apache.iotdb.tsfile.exception.encoding;

import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;

/**
 * This Exception is used while encoding failed. <br>
 * This Exception extends super class {@link TsFileRuntimeException}
 */
public class TsFileEncodingException extends TsFileRuntimeException {

  private static final long serialVersionUID = -7225811149696714845L;

  public TsFileEncodingException() {
    // do nothing
  }

  public TsFileEncodingException(String message, Throwable cause) {
    super(message, cause);
  }

  public TsFileEncodingException(String message) {
    super(message);
  }

  public TsFileEncodingException(Throwable cause) {
    super(cause);
  }
}
