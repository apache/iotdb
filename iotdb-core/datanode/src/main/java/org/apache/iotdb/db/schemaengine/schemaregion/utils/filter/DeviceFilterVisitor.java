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

package org.apache.iotdb.db.schemaengine.schemaregion.utils.filter;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.PathContainsFilter;
import org.apache.iotdb.commons.schema.filter.impl.TemplateFilter;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;

import static org.apache.iotdb.commons.conf.IoTDBConstant.STRING_NULL;

public class DeviceFilterVisitor extends SchemaFilterVisitor<IDeviceSchemaInfo> {
  @Override
  public boolean visitNode(SchemaFilter filter, IDeviceSchemaInfo info) {
    return true;
  }

  @Override
  public boolean visitPathContainsFilter(
      PathContainsFilter pathContainsFilter, IDeviceSchemaInfo info) {
    if (pathContainsFilter.getContainString() == null) {
      return true;
    }
    return info.getFullPath().toLowerCase().contains(pathContainsFilter.getContainString());
  }

  @Override
  public boolean visitTemplateFilter(TemplateFilter templateFilter, IDeviceSchemaInfo info) {
    if (templateFilter.getTemplateName() == null) {
      return true;
    }
    String templateName = STRING_NULL;
    boolean equalAns;
    int TemplateId = info.getTemplateId();
    if (TemplateId != -1) {
      templateName = ClusterTemplateManager.getInstance().getTemplate(TemplateId).getName();
      equalAns = templateName.equals(templateFilter.getTemplateName());
    } else {
      equalAns = templateName.equalsIgnoreCase(templateFilter.getTemplateName());
    }

    if (templateFilter.isEqual()) {
      return equalAns;
    } else {
      return !equalAns;
    }
  }
}
