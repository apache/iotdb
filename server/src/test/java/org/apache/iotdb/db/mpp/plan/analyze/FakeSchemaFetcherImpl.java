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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaEntityNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaInternalNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaComputationWithAutoCreation;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FakeSchemaFetcherImpl implements ISchemaFetcher {

  private final ClusterSchemaTree schemaTree = new ClusterSchemaTree(generateSchemaTree());

  @Override
  public ClusterSchemaTree fetchSchema(PathPatternTree patternTree) {
    schemaTree.setDatabases(Collections.singleton("root.sg"));
    return schemaTree;
  }

  @Override
  public ISchemaTree fetchSchemaWithTags(PathPatternTree patternTree) {
    return fetchSchema(patternTree);
  }

  @Override
  public void fetchAndComputeSchemaWithAutoCreate(
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation) {}

  @Override
  public void fetchAndComputeSchemaWithAutoCreate(
      List<? extends ISchemaComputationWithAutoCreation> schemaComputationWithAutoCreationList) {}

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

    SchemaMeasurementNode s1 =
        new SchemaMeasurementNode("s1", new MeasurementSchema("s1", TSDataType.INT32));
    s1.setTagMap(Collections.singletonMap("key1", "value1"));
    SchemaMeasurementNode s2 =
        new SchemaMeasurementNode("s2", new MeasurementSchema("s2", TSDataType.DOUBLE));
    s2.setTagMap(Collections.singletonMap("key1", "value1"));
    SchemaMeasurementNode s3 =
        new SchemaMeasurementNode("s3", new MeasurementSchema("s3", TSDataType.BOOLEAN));
    s3.setTagMap(Collections.singletonMap("key1", "value2"));
    SchemaMeasurementNode s4 =
        new SchemaMeasurementNode("s4", new MeasurementSchema("s4", TSDataType.TEXT));
    s4.setTagMap(Collections.singletonMap("key2", "value1"));
    s2.setAlias("status");

    SchemaEntityNode d1 = new SchemaEntityNode("d1");
    sg.addChild("d1", d1);
    d1.addChild("s1", s1);
    d1.addChild("s2", s2);
    d1.addAliasChild("status", s2);
    d1.addChild("s3", s3);

    SchemaEntityNode d2 = new SchemaEntityNode("d2");
    sg.addChild("d2", d2);
    d2.addChild("s1", s1);
    d2.addChild("s2", s2);
    d2.addAliasChild("status", s2);
    d2.addChild("s4", s4);

    SchemaEntityNode a = new SchemaEntityNode("a");
    a.setAligned(true);
    d2.addChild("a", a);
    a.addChild("s1", s1);
    a.addChild("s2", s2);
    a.addAliasChild("status", s2);

    return root;
  }

  @Override
  public ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath,
      List<String[]> measurements,
      List<TSDataType[]> tsDataTypes,
      List<TSEncoding[]> encodings,
      List<CompressionType[]> compressionTypes,
      List<Boolean> aligned) {
    return null;
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath path) {
    return null;
  }

  @Override
  public Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern) {
    return Collections.emptyMap();
  }

  @Override
  public Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName) {
    return null;
  }

  @Override
  public void invalidAllCache() {}
}
