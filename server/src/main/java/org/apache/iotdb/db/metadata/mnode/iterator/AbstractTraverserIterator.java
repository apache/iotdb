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
package org.apache.iotdb.db.metadata.mnode.iterator;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.IMTreeStore;
import org.apache.iotdb.db.metadata.template.Template;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.db.metadata.MetadataConstant.NON_TEMPLATE;

/**
 * ChildrenIterator for traverser. The iterator returns the filtered child nodes in the mtree and
 * the child nodes in the template.
 */
public abstract class AbstractTraverserIterator implements IMNodeIterator {
  private final IMNodeIterator directChildrenIterator;
  private Iterator<IMNode> templateChildrenIterator;
  private IMNode nextMatchedNode;
  // measurement in template should be processed only if templateMap is not null
  private Map<Integer, Template> templateMap;
  // if true, the pre deleted measurement or pre deactivated template won't be processed
  private boolean skipPreDeletedSchema = false;

  protected AbstractTraverserIterator(IMTreeStore store, IMNode parent) throws MetadataException {
    directChildrenIterator = store.getChildrenIterator(parent);
    if (templateMap != null && parent.isEntity() && parent.isUseTemplate()) {
      Template template = getActivatedSchemaTemplate(parent);
      if (template != null) {
        templateChildrenIterator = template.getDirectNodes().iterator();
      }
    }
  }

  public void setTemplateMap(Map<Integer, Template> templateMap) {
    this.templateMap = templateMap;
  }

  public void setSkipPreDeletedSchema(boolean skipPreDeletedSchema) {
    this.skipPreDeletedSchema = skipPreDeletedSchema;
  }

  private Template getActivatedSchemaTemplate(IMNode node) {
    // new cluster, the used template is directly recorded as template id in device mnode
    if (node.getSchemaTemplateId() != NON_TEMPLATE) {
      if (skipPreDeletedSchema && node.getAsEntityMNode().isPreDeactivateTemplate()) {
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
    if (nextMatchedNode == null) {
      while (true) {
        if (directChildrenIterator.hasNext()) {
          nextMatchedNode = directChildrenIterator.next();
        } else if (templateChildrenIterator != null && templateChildrenIterator.hasNext()) {
          nextMatchedNode = templateChildrenIterator.next();
        } else {
          break;
        }
        if (skipPreDeletedSchema
            && nextMatchedNode.isMeasurement()
            && nextMatchedNode.getAsMeasurementMNode().isPreDeleted()) {
          nextMatchedNode = null;
        }
      }
    }
    return nextMatchedNode != null;
  }

  @Override
  public IMNode next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    IMNode result = nextMatchedNode;
    nextMatchedNode = null;
    return result;
  }

  @Override
  public void close() {
    directChildrenIterator.close();
    templateChildrenIterator = null;
  }
}
