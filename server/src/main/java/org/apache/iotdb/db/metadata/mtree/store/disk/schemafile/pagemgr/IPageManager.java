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
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.pagemgr;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SegmentedPage;

import java.io.IOException;

/**
 * Framework to decouple {@link SchemaFile} from structure of {@link SchemaPage}. Various data structure
 * between {@link SchemaPage} could be implemented with this interface to compare performance.
 *
 * <p>Notice that {@link SegmentedPage} is not involved here since it only contains serialized IMNodes.
 *
 * <p>This interface completes three targets:
 * <ol>
 *   <li>find a target record(could be another key or pointer) with initial page and target key;
 *   <li>split a wrappedSegment and make up 3 new pages, one internal and two split, original abolished;
 *   <li>insert an entry into upper internal recursively.
 * </ol>
 * Notice every target maintains <b>an array tracing paths</b> seeking the target record.
 */
public interface IPageManager {

  ISchemaPage getPageInstance(int pageIdx) throws IOException, MetadataException;

  void flushDirtyPages() throws IOException;

  long getTargetSegmentAddress(long curAddr, String recKey) throws IOException, MetadataException;

  ISchemaPage getMinApplSegmentedPageInMem(short size);

}
