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

package org.apache.iotdb.db.queryengine.common.schematree.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.tree.AbstractTreeVisitor;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.queryengine.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.commons.schema.SchemaConstant.NON_TEMPLATE;

public abstract class SchemaTreeVisitor<R> extends AbstractTreeVisitor<SchemaNode, R> {

  private Map<Integer, Template> templateMap;

  protected SchemaTreeVisitor() {}

  protected SchemaTreeVisitor(SchemaNode root, PartialPath pathPattern, boolean isPrefixMatch) {
    super(root, pathPattern, isPrefixMatch);
    initStack();
  }

  protected SchemaTreeVisitor(
      SchemaNode root, PartialPath pathPattern, boolean isPrefixMatch, PathPatternTree scope) {
    super(root, pathPattern, isPrefixMatch, scope);
    initStack();
  }

  public List<R> getAllResult() {
    List<R> result = new ArrayList<>();
    while (hasNext()) {
      result.add(next());
    }
    return result;
  }

  public void setTemplateMap(Map<Integer, Template> templateMap) {
    this.templateMap = templateMap;
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(SchemaNode node) {
    return !node.isMeasurement();
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(SchemaNode node) {
    return !node.isMeasurement();
  }

  @Override
  protected SchemaNode getChild(SchemaNode parent, String childName) {
    if (templateMap != null
        && parent.isEntity()
        && parent.getAsEntityNode().getTemplateId() != NON_TEMPLATE) {
      Template template = templateMap.get(parent.getAsEntityNode().getTemplateId());
      if (template != null && template.getSchema(childName) != null) {
        return new SchemaMeasurementNode(childName, template.getSchema(childName));
      }
    }
    return parent.getChild(childName);
  }

  @Override
  protected Iterator<SchemaNode> getChildrenIterator(
      SchemaNode parent, Iterator<String> childrenName) throws Exception {
    return new Iterator<SchemaNode>() {
      private SchemaNode next = null;

      @Override
      public boolean hasNext() {
        if (next == null) {
          while (next == null && childrenName.hasNext()) {
            next = getChild(parent, childrenName.next());
          }
        }
        return next != null;
      }

      @Override
      public SchemaNode next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        SchemaNode result = next;
        next = null;
        return result;
      }
    };
  }

  @Override
  protected Iterator<SchemaNode> getChildrenIterator(SchemaNode parent) {
    if (templateMap != null
        && parent.isEntity()
        && parent.getAsEntityNode().getTemplateId() != NON_TEMPLATE) {
      return new Iterator<SchemaNode>() {
        private final Iterator<IMeasurementSchema> templateChildrenIterator =
            templateMap
                .get(parent.getAsEntityNode().getTemplateId())
                .getSchemaMap()
                .values()
                .iterator();
        private final Iterator<SchemaNode> directChildrenIterator = parent.getChildrenIterator();

        private SchemaNode next = null;

        @Override
        public boolean hasNext() {
          while (next == null) {
            if (directChildrenIterator.hasNext()) {
              next = directChildrenIterator.next();
            } else if (templateChildrenIterator.hasNext()) {
              IMeasurementSchema measurementSchema = templateChildrenIterator.next();
              next =
                  new SchemaMeasurementNode(
                      measurementSchema.getMeasurementName(), measurementSchema);
            } else {
              return false;
            }
          }
          return true;
        }

        @Override
        public SchemaNode next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          SchemaNode result = next;
          next = null;
          return result;
        }
      };
    } else {
      return parent.getChildrenIterator();
    }
  }
}
