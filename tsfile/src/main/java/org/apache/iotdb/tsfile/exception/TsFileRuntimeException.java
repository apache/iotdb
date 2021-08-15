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
package org.apache.iotdb.tsfile.exception;

/**
 * This Exception is the parent class for all runtime exceptions.<br>
 * This Exception extends super class {@link java.lang.RuntimeException}
 */
public class TsFileRuntimeException extends RuntimeException {

  private static final long serialVersionUID = 6455048223316780984L;

  public TsFileRuntimeException() {
    super();
  }

  public TsFileRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public TsFileRuntimeException(String message) {
    super(message);
  }

  public TsFileRuntimeException(Throwable cause) {
    super(cause);
  }
}
