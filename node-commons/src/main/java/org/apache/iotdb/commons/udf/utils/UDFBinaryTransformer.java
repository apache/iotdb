/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.udf.utils;

/**
 * Transform between {@link org.apache.iotdb.tsfile.utils.Binary} and {@link
 * org.apache.iotdb.udf.api.type.Binary}
 */
public class UDFBinaryTransformer {
  private UDFBinaryTransformer() {}

  public static org.apache.iotdb.tsfile.utils.Binary transformToBinary(
      org.apache.iotdb.udf.api.type.Binary binary) {
    return binary == null ? null : new org.apache.iotdb.tsfile.utils.Binary(binary.getValues());
  }

  public static org.apache.iotdb.udf.api.type.Binary transformToUDFBinary(
      org.apache.iotdb.tsfile.utils.Binary binary) {
    return binary == null ? null : new org.apache.iotdb.udf.api.type.Binary(binary.getValues());
  }
}
