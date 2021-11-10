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
package org.apache.iotdb.db.service.basic.dto;

import org.apache.iotdb.rpc.TSStatusCode;

public class BasicResp {

  private String message;
  private TSStatusCode tsStatusCode;

  public BasicResp() {}

  public BasicResp(TSStatusCode tsStatusCode, String message) {
    this.message = message;
    this.tsStatusCode = tsStatusCode;
  }

  public BasicResp(String message) {
    this.message = message;
  }

  public BasicResp(TSStatusCode tsStatusCode) {
    this.tsStatusCode = tsStatusCode;
  }

  public String getMessage() {
    return message;
  }

  public BasicResp message(String message) {
    this.message = message;
    return this;
  }

  public TSStatusCode getTsStatusCode() {
    return tsStatusCode;
  }

  public BasicResp tsStatusCode(TSStatusCode tsStatusCode) {
    this.tsStatusCode = tsStatusCode;
    return this;
  }
}
