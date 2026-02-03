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

package org.apache.iotdb.db.schemaengine.template;

import org.apache.iotdb.commons.schema.template.Template;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TemplateInternalRPCUtil {

  private TemplateInternalRPCUtil() {}

  public static byte[] generateAddTemplateSetInfoBytes(Template template, String templateSetPath) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(1, outputStream);
      template.serialize(outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write(templateSetPath, outputStream);
    } catch (IOException ignored) {
      // won't reach here
    }
    return outputStream.toByteArray();
  }

  public static Map<Template, List<String>> parseAddTemplateSetInfoBytes(ByteBuffer buffer) {
    int templateNum = ReadWriteIOUtils.readInt(buffer);
    Map<Template, List<String>> result = new HashMap<>(templateNum);
    int pathNum;
    List<String> templateSetPathList;
    for (int i = 0; i < templateNum; i++) {
      Template template = new Template();
      template.deserialize(buffer);

      pathNum = ReadWriteIOUtils.readInt(buffer);
      templateSetPathList = new ArrayList<>(pathNum);
      for (int j = 0; j < pathNum; j++) {
        templateSetPathList.add(ReadWriteIOUtils.readString(buffer));
      }
      result.put(template, templateSetPathList);
    }
    return result;
  }

  public static byte[] generateAddAllTemplateSetInfoBytes(
      Map<Template, List<Pair<String, Boolean>>> templateSetInfo) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(templateSetInfo.size(), outputStream);
      for (Map.Entry<Template, List<Pair<String, Boolean>>> entry : templateSetInfo.entrySet()) {
        entry.getKey().serialize(outputStream);
        ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
        for (Pair<String, Boolean> templateSetPath : entry.getValue()) {
          ReadWriteIOUtils.write(templateSetPath.left, outputStream);
          ReadWriteIOUtils.write(templateSetPath.right, outputStream);
        }
      }
    } catch (IOException ignored) {
      // won't reach here
    }
    return outputStream.toByteArray();
  }

  public static Map<Template, List<Pair<String, Boolean>>> parseAddAllTemplateSetInfoBytes(
      ByteBuffer buffer) {
    int templateNum = ReadWriteIOUtils.readInt(buffer);
    Map<Template, List<Pair<String, Boolean>>> result = new HashMap<>(templateNum);
    int pathNum;
    List<Pair<String, Boolean>> templateSetPathList;
    for (int i = 0; i < templateNum; i++) {
      Template template = new Template();
      template.deserialize(buffer);

      pathNum = ReadWriteIOUtils.readInt(buffer);
      templateSetPathList = new ArrayList<>(pathNum);
      for (int j = 0; j < pathNum; j++) {
        templateSetPathList.add(
            new Pair<>(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readBool(buffer)));
      }
      result.put(template, templateSetPathList);
    }
    return result;
  }

  public static byte[] generateInvalidateTemplateSetInfoBytes(int templateId, String path) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(templateId, outputStream);
      ReadWriteIOUtils.write(path, outputStream);
    } catch (IOException ignored) {
      // won't reach here
    }
    return outputStream.toByteArray();
  }

  public static Pair<Integer, String> parseInvalidateTemplateSetInfoBytes(ByteBuffer buffer) {
    return new Pair<>(ReadWriteIOUtils.readInt(buffer), ReadWriteIOUtils.readString(buffer));
  }

  public static byte[] generateUpdateTemplateInfoBytes(Template template) {
    return template.serialize().array();
  }

  public static Template parseUpdateTemplateInfoBytes(ByteBuffer buffer) {
    Template template = new Template();
    template.deserialize(buffer);
    return template;
  }
}
