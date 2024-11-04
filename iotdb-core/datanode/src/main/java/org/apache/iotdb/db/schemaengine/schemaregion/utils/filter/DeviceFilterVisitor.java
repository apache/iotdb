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
import org.apache.iotdb.commons.schema.filter.impl.StringValueFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.TemplateFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.AttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;

import org.apache.tsfile.common.conf.TSFileConfig;

public class DeviceFilterVisitor extends SchemaFilterVisitor<IDeviceSchemaInfo> {

  @Override
  public Boolean visitNode(final SchemaFilter filter, final IDeviceSchemaInfo info) {
    return true;
  }

  @Override
  public Boolean visitPathContainsFilter(
      final PathContainsFilter pathContainsFilter, final IDeviceSchemaInfo info) {
    if (pathContainsFilter.getContainString() == null) {
      return true;
    }
    return info.getFullPath().toLowerCase().contains(pathContainsFilter.getContainString());
  }

  @Override
  public Boolean visitTemplateFilter(
      final TemplateFilter templateFilter, final IDeviceSchemaInfo info) {
    final boolean equalAns;
    final int templateId = info.getTemplateId();
    final String filterTemplateName = templateFilter.getTemplateName();
    if (templateId != -1) {
      equalAns =
          ClusterTemplateManager.getInstance()
              .getTemplate(templateId)
              .getName()
              .equals(filterTemplateName);
      return templateFilter.isEqual() == equalAns;
    } else if (filterTemplateName == null) {
      return templateFilter.isEqual();
    } else {
      return false;
    }
  }

  @Override
  public Boolean visitIdFilter(final IdFilter filter, final IDeviceSchemaInfo info) {
    final String[] nodes = info.getPartialPath().getNodes();
    return filter
        .getChild()
        .accept(
            StringValueFilterVisitor.getInstance(),
            nodes.length > filter.getIndex() + 3 ? nodes[filter.getIndex() + 3] : null);
  }

  @Override
  public Boolean visitAttributeFilter(final AttributeFilter filter, final IDeviceSchemaInfo info) {
    return filter
        .getChild()
        .accept(
            StringValueFilterVisitor.getInstance(),
            info.getAttributeValue(filter.getKey()).getStringValue(TSFileConfig.STRING_CHARSET));
  }
}
