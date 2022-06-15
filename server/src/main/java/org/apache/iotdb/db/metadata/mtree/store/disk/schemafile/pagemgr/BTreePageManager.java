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
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Queue;

import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.INTERNAL_SPLIT_VALVE;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getGlobalIndex;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getNodeAddress;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getPageIndex;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getSegIndex;

public class BTreePageManager extends PageManager {
  private BTreePageManager(FileChannel channel, int lpi) throws IOException, MetadataException {
    super(channel, lpi);
  }

  public static IPageManager getBTreePageManager(FileChannel channel, int lastPageIdx)
      throws IOException, MetadataException {
    return new BTreePageManager(channel, lastPageIdx);
  }

  @Override
  protected void multiPageInsertOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer)
      throws MetadataException, IOException {
    // cur is a leaf, split and set next ptr as link between
    ISchemaPage newPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
    newPage.getAsSegmentedPage().allocNewSegment(SchemaPage.SEG_MAX_SIZ);
    String sk =
        curPage
            .getAsSegmentedPage()
            .splitWrappedSegment(key, childBuffer, newPage, SchemaPage.INCLINED_SPLIT);
    curPage
        .getAsSegmentedPage()
        .setNextSegAddress((short) 0, getGlobalIndex(newPage.getPageIndex(), (short) 0));
    if (treeTrace[0] < 1) {
      // first leaf to split and transplant, so that curSegAddr stay unchanged
      ISchemaPage trsPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
      trsPage
          .getAsSegmentedPage()
          .transplantSegment(curPage.getAsSegmentedPage(), (short) 0, SchemaPage.SEG_MAX_SIZ);
      ISchemaPage repPage =
          ISchemaPage.initInternalPage(
              ByteBuffer.allocate(SchemaPage.PAGE_LENGTH),
              curPage.getPageIndex(),
              trsPage.getPageIndex());

      repPage.getAsInternalPage().insertRecord(sk, newPage.getPageIndex());
      repPage
          .getAsInternalPage()
          .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));
      replacePageInCache(repPage);

      dirtyPages.put(trsPage.getPageIndex(), trsPage);
    } else {
      insertIndexEntry(treeTrace[0], sk, newPage.getPageIndex());
      dirtyPages.put(curPage.getPageIndex(), curPage);
    }
  }

  /**
   * Insert an index entry into an internal page. Cascade insert or internal split conducted if
   * necessary.
   *
   * @param key key of the entry
   * @param ptr pointer of the entry
   */
  private void insertIndexEntry(int treeTraceIndex, String key, int ptr)
      throws MetadataException, IOException {
    ISchemaPage iPage = getPageInstance(treeTrace[treeTraceIndex]);
    if (iPage.getAsInternalPage().insertRecord(key, ptr) < INTERNAL_SPLIT_VALVE) {
      if (treeTraceIndex > 1) {
        // overflow, but parent exists
        ByteBuffer dstBuffer = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);
        String sk =
            iPage.getAsInternalPage().splitByKey(key, ptr, dstBuffer, SchemaPage.INCLINED_SPLIT);
        ISchemaPage dstPage = ISchemaPage.loadSchemaPage(dstBuffer);
        registerAsNewPage(dstPage);
        insertIndexEntry(treeTraceIndex - 1, sk, dstPage.getPageIndex());
      } else {
        // split as root internal
        ByteBuffer splBuffer = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);
        ByteBuffer trsBuffer = ByteBuffer.allocate(SchemaPage.PAGE_LENGTH);
        String sk =
            iPage.getAsInternalPage().splitByKey(key, ptr, splBuffer, SchemaPage.INCLINED_SPLIT);
        iPage.getAsInternalPage().extendsTo(trsBuffer);
        ISchemaPage splPage = ISchemaPage.loadSchemaPage(splBuffer);
        ISchemaPage trsPage = ISchemaPage.loadSchemaPage(trsBuffer);
        registerAsNewPage(splPage);
        registerAsNewPage(trsPage);

        iPage.getAsInternalPage().resetBuffer(trsPage.getPageIndex());
        if (iPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex()) < 0) {
          throw new MetadataException("Key too large to store in a new InternalSegment: " + sk);
        }
        iPage
            .getAsInternalPage()
            .setNextSegAddress(trsPage.getAsInternalPage().getNextSegAddress());
      }
    }
    dirtyPages.put(iPage.getPageIndex(), iPage);
  }

  @Override
  protected void multiPageUpdateOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer)
      throws MetadataException, IOException {
    // split and update higer nodes
    ISchemaPage splPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
    splPage.getAsSegmentedPage().allocNewSegment(SchemaPage.SEG_MAX_SIZ);
    String sk = curPage.getAsSegmentedPage().splitWrappedSegment(null, null, splPage, false);
    curPage
        .getAsSegmentedPage()
        .setNextSegAddress((short) 0, getGlobalIndex(splPage.getPageIndex(), (short) 0));

    // update on page where it exists
    if (key.compareTo(sk) >= 0) {
      splPage.getAsSegmentedPage().update((short) 0, key, childBuffer);
    } else {
      curPage.getAsSegmentedPage().update((short) 0, key, childBuffer);
    }

    // insert index entry upward
    if (treeTrace[0] < 1) {
      ISchemaPage trsPage = getMinApplSegmentedPageInMem(SchemaPage.SEG_MAX_SIZ);
      trsPage
          .getAsSegmentedPage()
          .transplantSegment(curPage.getAsSegmentedPage(), (short) 0, SchemaPage.SEG_MAX_SIZ);
      ISchemaPage repPage =
          ISchemaPage.initInternalPage(
              ByteBuffer.allocate(SchemaPage.PAGE_LENGTH),
              curPage.getPageIndex(),
              trsPage.getPageIndex());

      repPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex());
      repPage
          .getAsInternalPage()
          .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));
      replacePageInCache(repPage);

      dirtyPages.put(trsPage.getPageIndex(), trsPage);
    } else {
      insertIndexEntry(treeTrace[0], sk, splPage.getPageIndex());
      dirtyPages.put(curPage.getPageIndex(), curPage);
    }
  }

  @Override
  public void delete(IMNode node) throws IOException, MetadataException {
    long recSegAddr = node.getParent() == null ? 0L : getNodeAddress(node.getParent());
    recSegAddr = getTargetSegmentAddress(recSegAddr, node.getName());
    ISchemaPage tarPage = getPageInstance(getPageIndex(recSegAddr));
    dirtyPages.putIfAbsent(tarPage.getPageIndex(), tarPage);
    tarPage.getAsSegmentedPage().removeRecord(getSegIndex(recSegAddr), node.getName());

    if (!node.isMeasurement()) {
      long delSegAddr = getNodeAddress(node);
      tarPage = getPageInstance(getPageIndex(delSegAddr));
      dirtyPages.putIfAbsent(tarPage.getPageIndex(), tarPage);
      tarPage.getAsSegmentedPage().deleteSegment(getSegIndex(delSegAddr));
    }

    flushDirtyPages();
  }

  @Override
  public IMNode getChildNode(IMNode parent, String childName)
      throws MetadataException, IOException {
    if (getNodeAddress(parent) < 0) {
      throw new MetadataException(
          String.format(
              "Node [%s] has no valid segment address in schema file.", parent.getFullPath()));
    }

    long actualSegAddr = getTargetSegmentAddress(getNodeAddress(parent), childName);
    IMNode child =
        getPageInstance(getPageIndex(actualSegAddr))
            .getAsSegmentedPage()
            .read(getSegIndex(actualSegAddr), childName);

    // seek for alias if parent is entity, TODO: improve efficiency
    if (child == null && parent.isEntity()) {
      return getChildWithAlias(parent, childName);
    }
    return child;
  }

  private IMNode getChildWithAlias(IMNode par, String alias) throws IOException, MetadataException {
    long srtAddr = getNodeAddress(par);
    ISchemaPage page = getPageInstance(getPageIndex(srtAddr));

    while (page.getAsSegmentedPage() == null) {
      page = getPageInstance(getPageIndex(page.getAsInternalPage().getNextSegAddress()));
    }

    IMNode res = page.getAsSegmentedPage().readByAlias(getSegIndex(srtAddr), alias);

    // TODO: now it traverses all segments, improve with another index struct
    while (res == null && page.getAsSegmentedPage().getNextSegAddress(getSegIndex(srtAddr)) >= 0) {
      page =
          getPageInstance(
              getPageIndex(page.getAsSegmentedPage().getNextSegAddress(getSegIndex(srtAddr))));
      res = page.getAsSegmentedPage().readByAlias(getSegIndex(srtAddr), alias);
    }

    return res;
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException {
    int pageIdx = getPageIndex(getNodeAddress(parent));
    short segId = getSegIndex(getNodeAddress(parent));
    ISchemaPage page = getPageInstance(pageIdx);

    while (page.getAsSegmentedPage() == null) {
      page = getPageInstance(getPageIndex(page.getAsInternalPage().getNextSegAddress()));
    }

    long actualSegAddr = page.getAsSegmentedPage().getNextSegAddress(segId);
    Queue<IMNode> initChildren = page.getAsSegmentedPage().getChildren(segId);
    return new Iterator<IMNode>() {
      long nextSeg = actualSegAddr;
      Queue<IMNode> children = initChildren;

      @Override
      public boolean hasNext() {
        if (children.size() > 0) {
          return true;
        }
        if (nextSeg < 0) {
          return false;
        }

        try {
          ISchemaPage nPage = getPageInstance(getPageIndex(nextSeg));
          children = nPage.getAsSegmentedPage().getChildren(getSegIndex(nextSeg));
          nextSeg = nPage.getAsSegmentedPage().getNextSegAddress(getSegIndex(nextSeg));
        } catch (MetadataException | IOException e) {
          return false;
        }

        return children.size() > 0;
      }

      @Override
      public IMNode next() {
        return children.poll();
      }
    };
  }

  @Override
  protected long getTargetSegmentAddress(long curSegAddr, String recKey)
      throws IOException, MetadataException {
    treeTrace[0] = 0;
    ISchemaPage curPage = getPageInstance(getPageIndex(curSegAddr));
    if (curPage.getAsSegmentedPage() != null) {
      return curSegAddr;
    }

    int i = 0; // mark the trace of b+ tree node
    while (curPage.getAsInternalPage() != null) {
      i++;
      treeTrace[i] = curPage.getPageIndex();
      curPage = getPageInstance(curPage.getAsInternalPage().getRecordByKey(recKey));
    }
    treeTrace[0] = i; // bound in no.0 elem, points the parent the return

    return getGlobalIndex(curPage.getPageIndex(), (short) 0);
  }
}
