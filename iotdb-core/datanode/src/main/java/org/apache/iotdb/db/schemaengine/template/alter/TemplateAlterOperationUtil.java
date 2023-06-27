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

package org.apache.iotdb.db.schemaengine.template.alter;

import org.apache.iotdb.db.schemaengine.template.TemplateAlterOperationType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TemplateAlterOperationUtil {

  private TemplateAlterOperationUtil() {
    // not allowed construction
  }

  public static byte[] generateExtendTemplateReqInfo(
      TemplateAlterOperationType operationType, TemplateAlterInfo templateAlterInfo) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      operationType.serialize(outputStream);
      templateAlterInfo.serialize(outputStream);
    } catch (IOException ignored) {
      // won't reach here
    }
    return outputStream.toByteArray();
  }

  public static TemplateAlterOperationType parseOperationType(ByteBuffer buffer) {
    return TemplateAlterOperationType.deserialize(buffer);
  }

  public static TemplateExtendInfo parseTemplateExtendInfo(ByteBuffer buffer) {
    TemplateExtendInfo templateExtendInfo = new TemplateExtendInfo();
    templateExtendInfo.deserialize(buffer);
    return templateExtendInfo;
  }
}
