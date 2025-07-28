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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.iterator;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MNodeUtils;
import org.apache.iotdb.db.schemaengine.template.Template;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

/**
 * TraverserIterator for traversing device node. The iterator returns the filtered child nodes in
 * the mtree and the child nodes in the template.
 */
public abstract class AbstractTraverserIterator<N extends IMNode<N>> implements IMNodeIterator<N> {
  private final IMNodeIterator<N> directChildrenIterator;
  private Iterator<N> templateChildrenIterator;
  protected N nextMatchedNode;
  protected boolean usingDirectChildrenIterator = true;
  // if true, the pre deleted measurement or pre deactivated template won't be processed
  private boolean skipPreDeletedSchema = false;

  private boolean skipTemplateChildren = false;

  protected AbstractTraverserIterator(
      IMTreeStore<N> store,
      IDeviceMNode<N> parent,
      Map<Integer, Template> templateMap,
      IMNodeFactory<N> nodeFactory)
      throws MetadataException {
    this.directChildrenIterator = store.getChildrenIterator(parent.getAsMNode());
    if (templateMap != null && parent.isUseTemplate()) {
      Template template = getActivatedSchemaTemplate(parent, templateMap);
      if (template != null) {
        templateChildrenIterator = MNodeUtils.getChildren(template, nodeFactory);
      }
    }
  }

  public void setSkipPreDeletedSchema(boolean skipPreDeletedSchema) {
    this.skipPreDeletedSchema = skipPreDeletedSchema;
  }

  @Override
  public void skipTemplateChildren() {
    skipTemplateChildren = true;
  }

  private Template getActivatedSchemaTemplate(
      IDeviceMNode<N> node, Map<Integer, Template> templateMap) {
    // new cluster, the used template is directly recorded as template id in device mnode
    if (node.getSchemaTemplateId() != NON_TEMPLATE) {
      if (skipPreDeletedSchema && node.getAsDeviceMNode().isPreDeactivateSelfOrTemplate()) {
        // skip this pre deactivated template, the invoker will skip this
        return null;
      }
      return templateMap.get(node.getSchemaTemplateId());
    }
    // if the node is usingTemplate, the upperTemplate won't be null or the upperTemplateId won't be
    // NON_TEMPLATE.
    throw new IllegalStateException(
        String.format(
            "There should be a template mounted on any ancestor of the node [%s] usingTemplate.",
            node.getFullPath()));
  }

  @Override
  public boolean hasNext() {
    while (nextMatchedNode == null) {
      if (directChildrenIterator.hasNext()) {
        nextMatchedNode = directChildrenIterator.next();
        usingDirectChildrenIterator = true;
      } else if (!skipTemplateChildren
          && templateChildrenIterator != null
          && templateChildrenIterator.hasNext()) {
        nextMatchedNode = templateChildrenIterator.next();
        usingDirectChildrenIterator = false;
      } else {
        return false;
      }
      if (skipPreDeletedSchema
          && nextMatchedNode.isMeasurement()
          && nextMatchedNode.getAsMeasurementMNode().isPreDeleted()) {
        nextMatchedNode = null;
      }
    }
    return true;
  }

  @Override
  public N next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    N result = nextMatchedNode;
    nextMatchedNode = null;
    return result;
  }

  @Override
  public void close() {
    directChildrenIterator.close();
    templateChildrenIterator = null;
  }
}
