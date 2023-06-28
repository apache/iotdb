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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaPage;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

/**
 * Framework to decouple {@link SchemaFile} from structure of {@link SchemaPage}. Various data
 * structure between {@link SchemaPage} could be implemented under this interface with respect to
 * the requirement.
 *
 * <p>{@linkplain SchemaFile} only needs to handle initialization and header content, and interact
 * with this Manager ignoring pages since this interface implements all read and write methods about
 * pages.
 */
public interface IPageManager {

  void writeNewChildren(ICachedMNode parNode) throws MetadataException, IOException;

  void writeUpdatedChildren(ICachedMNode parNode) throws MetadataException, IOException;

  void delete(ICachedMNode node) throws IOException, MetadataException;

  ICachedMNode getChildNode(ICachedMNode parent, String childName)
      throws MetadataException, IOException;

  Iterator<ICachedMNode> getChildren(ICachedMNode parent) throws MetadataException, IOException;

  void clear() throws IOException, MetadataException;

  void flushDirtyPages() throws IOException;

  void close() throws IOException;

  int getLastPageIndex();

  void inspect(PrintWriter pw) throws IOException, MetadataException;
}
