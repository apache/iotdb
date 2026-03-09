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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class TemplatePreSetTableTest {

  private TemplatePreSetTable templatePreSetTable;
  private final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @Before
  public void setup() throws IOException {
    templatePreSetTable = new TemplatePreSetTable();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @After
  public void cleanup() throws IOException {
    templatePreSetTable = null;
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testPreSetTemplate() throws IllegalPathException {
    int templateId1 = 5;
    int templateId2 = 10;
    PartialPath templateSetPath1 = new PartialPath("root.db.t1");
    PartialPath templateSetPath2 = new PartialPath("root.db.t2");
    Assert.assertFalse(templatePreSetTable.isPreSet(templateId1, templateSetPath1));
    Assert.assertFalse(templatePreSetTable.removeSetTemplate(templateId1, templateSetPath1));
    Assert.assertFalse(templatePreSetTable.isPreSet(templateId2, templateSetPath1));
    Assert.assertFalse(templatePreSetTable.removeSetTemplate(templateId2, templateSetPath1));

    templatePreSetTable.preSetTemplate(templateId1, templateSetPath1);
    templatePreSetTable.preSetTemplate(templateId2, templateSetPath1);
    templatePreSetTable.preSetTemplate(templateId2, templateSetPath2);

    Assert.assertTrue(templatePreSetTable.isPreSet(templateId1, templateSetPath1));
    Assert.assertTrue(templatePreSetTable.isPreSet(templateId2, templateSetPath1));
    Assert.assertTrue(templatePreSetTable.isPreSet(templateId2, templateSetPath2));

    Assert.assertTrue(templatePreSetTable.removeSetTemplate(templateId1, templateSetPath1));
    Assert.assertTrue(templatePreSetTable.removeSetTemplate(templateId2, templateSetPath1));

    Assert.assertFalse(templatePreSetTable.isPreSet(templateId1, templateSetPath1));
    Assert.assertFalse(templatePreSetTable.isPreSet(templateId2, templateSetPath1));
    Assert.assertTrue(templatePreSetTable.isPreSet(templateId2, templateSetPath2));
  }

  @Test
  public void testSnapshot() throws IllegalPathException {
    int templateId1 = 5;
    int templateId2 = 10;
    PartialPath templateSetPath1 = new PartialPath("root.db.t1");
    PartialPath templateSetPath2 = new PartialPath("root.db.t2");

    try {
      templatePreSetTable.processTakeSnapshot(snapshotDir);
      TemplatePreSetTable newTemplatePreSetTable = new TemplatePreSetTable();

      Assert.assertFalse(newTemplatePreSetTable.isPreSet(templateId1, templateSetPath1));
      Assert.assertFalse(newTemplatePreSetTable.isPreSet(templateId2, templateSetPath1));
      Assert.assertFalse(newTemplatePreSetTable.isPreSet(templateId2, templateSetPath2));

      templatePreSetTable.preSetTemplate(templateId1, templateSetPath1);
      templatePreSetTable.preSetTemplate(templateId2, templateSetPath1);
      templatePreSetTable.preSetTemplate(templateId2, templateSetPath2);

      templatePreSetTable.processTakeSnapshot(snapshotDir);
      newTemplatePreSetTable = new TemplatePreSetTable();
      newTemplatePreSetTable.processLoadSnapshot(snapshotDir);

      Assert.assertTrue(templatePreSetTable.isPreSet(templateId1, templateSetPath1));
      Assert.assertTrue(templatePreSetTable.isPreSet(templateId2, templateSetPath1));
      Assert.assertTrue(templatePreSetTable.isPreSet(templateId2, templateSetPath2));
    } catch (IOException e) {
      Assert.fail();
    }
  }
}
