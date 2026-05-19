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

package org.apache.iotdb.db.i18n;

import org.slf4j.helpers.MessageFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataNodeSchemaMessagesZhFormatTest {

  private static final String ZH_MESSAGES_RELATIVE_PATH =
      "src/main/i18n/zh/org/apache/iotdb/db/i18n/DataNodeSchemaMessages.java";

  @Test
  public void testZhSlf4jTemplatesPreserveLoggerArgumentOrder() throws IOException {
    Assert.assertEquals(
        "从 mlog.bin 反序列化 MTree 耗时 12 ms，对应路径 root.sg",
        formatSlf4j(readZhMessage("SPEND_TIME_DESERIALIZE_MTREE"), 12, "root.sg"));
    Assert.assertEquals(
        "跳过 128 失败，schemaRegion 目录为 C:/schema",
        formatSlf4j(readZhMessage("FAILED_TO_SKIP_MLOG"), 128, "C:/schema"));
    Assert.assertEquals(
        "恢复 root.sg.d.s 的 tagIndex 失败，schemaRegion 为 SchemaRegionId{1}。",
        formatSlf4j(
            readZhMessage("FAILED_TO_RECOVER_TAG_INDEX"),
            "root.sg.d.s",
            "SchemaRegionId{1}"));
    Assert.assertEquals(
        "MTree 刷写耗时 321ms，SchemaRegion 为 SchemaRegionId{1}",
        formatSlf4j(readZhMessage("MTREE_FLUSH_COST"), 321, "SchemaRegionId{1}"));
  }

  @Test
  public void testZhStringFormatTemplatePreservesArgumentTypesAndOrder() throws IOException {
    Assert.assertEquals(
        "SchemaRegion [7] 在 StorageGroup [root.sg] 中恢复失败。",
        String.format(readZhMessage("SCHEMA_REGION_FAILED_TO_RECOVER"), 7, "root.sg"));
  }

  private static String formatSlf4j(String template, Object... arguments) {
    return MessageFormatter.arrayFormat(template, arguments).getMessage();
  }

  private static String readZhMessage(String constantName) throws IOException {
    Path basePath = Paths.get(ZH_MESSAGES_RELATIVE_PATH);
    if (!Files.exists(basePath)) {
      basePath = Paths.get("iotdb-core", "datanode", ZH_MESSAGES_RELATIVE_PATH);
    }
    Assert.assertTrue("Missing zh messages file: " + basePath, Files.exists(basePath));

    String content = new String(Files.readAllBytes(basePath), StandardCharsets.UTF_8);
    Matcher matcher =
        Pattern.compile(
                "public static final String\\s+"
                    + Pattern.quote(constantName)
                    + "\\s*=\\s*\"([^\"]*)\";",
                Pattern.DOTALL)
            .matcher(content);
    Assert.assertTrue("Missing zh message constant: " + constantName, matcher.find());
    return matcher.group(1);
  }
}
