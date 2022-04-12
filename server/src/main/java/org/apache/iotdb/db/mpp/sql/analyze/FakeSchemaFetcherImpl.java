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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;

public class FakeSchemaFetcherImpl implements ISchemaFetcher {

  private final SchemaTree schemaTree = new SchemaTree(generateSchemaTree());

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree) {
    return schemaTree;
  }

  @Override
  public SchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes) {
    return schemaTree;
  }

  /**
   * Generate the following tree: root.sg.d1.s1, root.sg.d1.s2(status) root.sg.d2.s1,
   * root.sg.d2.s2(status) root.sg.d2.a.s1, root.sg.d2.a.s2(status)
   *
   * @return the root node of the generated schemTree
   */
  private SchemaNode generateSchemaTree() {
    SchemaNode root = new SchemaInternalNode("root");

    SchemaNode sg = new SchemaInternalNode("sg");
    root.addChild("sg", sg);

    SchemaEntityNode d1 = new SchemaEntityNode("d1");
    sg.addChild("d1", d1);

    SchemaMeasurementNode s1 =
        new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32));
    d1.addChild("s1", s1);
    SchemaMeasurementNode s2 =
        new SchemaMeasurementNode("s2", new MeasurementSchema("s1", TSDataType.INT32));
    s2.setAlias("status");
    d1.addChild("s2", s2);
    d1.addAliasChild("status", s2);

    SchemaEntityNode d2 = new SchemaEntityNode("d2");
    sg.addChild("d2", d2);
    d2.addChild("s1", s1);
    d2.addChild("s2", s2);
    d2.addAliasChild("status", s2);

    SchemaEntityNode a = new SchemaEntityNode("a");
    a.setAligned(true);
    d2.addChild("a", a);
    a.addChild("s1", s1);
    a.addChild("s2", s2);
    a.addAliasChild("status", s2);

    return root;
  }

  @Override
  public SchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath, List<String[]> measurements, List<TSDataType[]> tsDataTypes) {
    return null;
  }
}
