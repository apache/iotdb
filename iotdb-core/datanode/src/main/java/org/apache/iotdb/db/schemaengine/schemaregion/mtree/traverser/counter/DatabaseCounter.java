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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.counter;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.basic.DatabaseTraverser;

import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE_PATH;

// This class implement database counter.
public class DatabaseCounter<N extends IMNode<N>> extends DatabaseTraverser<Void, N>
    implements Counter {

  private int count;
  private final boolean needAuditDB;

  public DatabaseCounter(
      N startNode,
      PartialPath path,
      IMTreeStore<N> store,
      boolean isPrefixMatch,
      PathPatternTree scope,
      boolean needAuditDB)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch, scope);
    this.needAuditDB = needAuditDB;
  }

  @Override
  protected Void generateResult(N nextMatchedNode) {
    count++;
    if (!needAuditDB) {
      PartialPath dbName = nextMatchedNode.getAsDatabaseMNode().getPartialPath();
      if (TREE_MODEL_AUDIT_DATABASE_PATH.equals(dbName)) {
        count--;
      }
    }
    return null;
  }

  @Override
  public long count() throws MetadataException {
    while (hasNext()) {
      next();
    }
    if (!isSuccess()) {
      Throwable e = getFailure();
      throw new MetadataException(e.getMessage(), e);
    }
    return count;
  }
}
