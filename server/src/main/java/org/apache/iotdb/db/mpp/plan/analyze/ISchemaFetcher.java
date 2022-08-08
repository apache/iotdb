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
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;

/**
 * This interface is used to fetch the metadata information required in execution plan generating.
 */
public interface ISchemaFetcher {

  ISchemaTree fetchSchema(PathPatternTree patternTree);

  ISchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean aligned);

  ISchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath,
      List<String[]> measurements,
      List<TSDataType[]> tsDataTypes,
      List<Boolean> aligned);

  Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath path);

  Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern);

  Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName);

  void invalidAllCache();
}
