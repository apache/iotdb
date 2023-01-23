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
import org.apache.iotdb.db.exception.metadata.schemafile.ColossalRecordException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISegmentedPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;

import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getGlobalIndex;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getNodeAddress;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getPageIndex;
import static org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile.getSegIndex;

public class BTreePageManager extends PageManager {

  public BTreePageManager(FileChannel channel, File pmtFile, int lastPageIndex, String logPath)
      throws IOException, MetadataException {
    super(channel, pmtFile, lastPageIndex, logPath);
  }

  @Override
  protected void multiPageInsertOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer)
      throws MetadataException, IOException {
    // cur is a leaf, split and set next ptr as link between
    ISegmentedPage newPage = getMinApplSegmentedPageInMem(SchemaFileConfig.SEG_MAX_SIZ);
    newPage.allocNewSegment(SchemaFileConfig.SEG_MAX_SIZ);
    String sk =
        curPage
            .getAsSegmentedPage()
            .splitWrappedSegment(key, childBuffer, newPage, SchemaFileConfig.INCLINED_SPLIT);
    curPage
        .getAsSegmentedPage()
        .setNextSegAddress((short) 0, getGlobalIndex(newPage.getPageIndex(), (short) 0));
    markDirty(curPage);
    insertIndexEntryEntrance(curPage, newPage, sk);
  }

  @Override
  protected void multiPageUpdateOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer)
      throws MetadataException, IOException {
    // split and update higer nodes
    ISegmentedPage splPage = getMinApplSegmentedPageInMem(SchemaFileConfig.SEG_MAX_SIZ);
    splPage.allocNewSegment(SchemaFileConfig.SEG_MAX_SIZ);
    String sk = curPage.getAsSegmentedPage().splitWrappedSegment(null, null, splPage, false);
    curPage
        .getAsSegmentedPage()
        .setNextSegAddress((short) 0, getGlobalIndex(splPage.getPageIndex(), (short) 0));

    // update on page where it exists
    if (key.compareTo(sk) >= 0) {
      splPage.update((short) 0, key, childBuffer);
    } else {
      curPage.getAsSegmentedPage().update((short) 0, key, childBuffer);
    }

    // insert index entry upward
    insertIndexEntryEntrance(curPage, splPage, sk);
  }

  /**
   * In this implementation, subordinate index builds on alias.
   *
   * @param parNode node needs to build subordinate index.
   * @throws MetadataException
   * @throws IOException
   */
  @Override
  protected void buildSubIndex(IMNode parNode) throws MetadataException, IOException {
    ISchemaPage cursorPage = getPageInstance(getPageIndex(getNodeAddress(parNode)));

    if (cursorPage.getAsInternalPage() == null) {
      throw new MetadataException("Subordinate index shall not build upon single page segment.");
    }

    ISchemaPage tPage = cursorPage; // reserve the top page to modify subIndex
    ISchemaPage subIndexPage =
        ISchemaPage.initAliasIndexPage(ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), -1);
    registerAsNewPage(subIndexPage);

    // transfer cursorPage to leaf page
    while (cursorPage.getAsInternalPage() != null) {
      cursorPage =
          getPageInstance(getPageIndex(cursorPage.getAsInternalPage().getNextSegAddress()));
    }

    long nextAddr = cursorPage.getAsSegmentedPage().getNextSegAddress((short) 0);
    Queue<IMNode> children = cursorPage.getAsSegmentedPage().getChildren((short) 0);
    IMNode child;
    // TODO: inefficient to build B+Tree up-to-bottom, improve further
    while (!children.isEmpty() || nextAddr != -1L) {
      if (children.isEmpty()) {
        cursorPage = getPageInstance(getPageIndex(nextAddr));
        nextAddr = cursorPage.getAsSegmentedPage().getNextSegAddress((short) 0);
        children = cursorPage.getAsSegmentedPage().getChildren((short) 0);
      }
      child = children.poll();
      if (child != null
          && child.isMeasurement()
          && child.getAsMeasurementMNode().getAlias() != null) {
        subIndexPage =
            insertAliasIndexEntry(
                subIndexPage, child.getAsMeasurementMNode().getAlias(), child.getName());
      }
    }

    tPage.setSubIndex(subIndexPage.getPageIndex());
  }

  @Override
  protected void insertSubIndexEntry(int base, String key, String rec)
      throws MetadataException, IOException {
    insertAliasIndexEntry(getPageInstance(base), key, rec);
  }

  @Override
  protected void removeSubIndexEntry(int base, String oldAlias)
      throws MetadataException, IOException {
    ISchemaPage tarPage = getTargetLeafPage(getPageInstance(base), oldAlias);
    tarPage.getAsAliasIndexPage().removeRecord(oldAlias);
  }

  @Override
  protected String searchSubIndexAlias(int base, String alias)
      throws MetadataException, IOException {
    return getTargetLeafPage(getPageInstance(base), alias)
        .getAsAliasIndexPage()
        .getRecordByAlias(alias);
  }

  /** @return top page to insert index */
  private ISchemaPage insertAliasIndexEntry(ISchemaPage topPage, String alias, String name)
      throws MetadataException, IOException {
    ISchemaPage tarPage = getTargetLeafPage(topPage, alias);
    if (tarPage.getAsAliasIndexPage() == null) {
      throw new MetadataException("File may be corrupted that subordinate index has broken.");
    }

    if (tarPage.getAsAliasIndexPage().insertRecord(alias, name) < 0) {
      // need split and upwards insert
      ByteBuffer spltBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
      String sk =
          tarPage
              .getAsAliasIndexPage()
              .splitByKey(alias, name, spltBuf, SchemaFileConfig.INCLINED_SPLIT);
      ISchemaPage splPage = ISchemaPage.loadSchemaPage(spltBuf);
      registerAsNewPage(splPage);

      if (treeTrace[0] < 1) {
        // from single sub-index page to tree structure
        ByteBuffer trsBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
        tarPage.getAsAliasIndexPage().extendsTo(trsBuf);
        ISchemaPage trsPage = ISchemaPage.loadSchemaPage(trsBuf);

        // notice that index of tarPage belongs to repPage then
        registerAsNewPage(trsPage);

        // tarPage abolished since then
        ISchemaPage repPage =
            ISchemaPage.initInternalPage(
                ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH),
                tarPage.getPageIndex(),
                trsPage.getPageIndex());

        if (0 > repPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex())) {
          throw new ColossalRecordException(sk, alias);
        }

        repPage
            .getAsInternalPage()
            .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));

        replacePageInCache(repPage);
        return repPage;
      } else {
        insertIndexEntryRecursiveUpwards(treeTrace[0], sk, splPage.getPageIndex());
        return getPageInstance(treeTrace[1]);
      }
    }
    return topPage;
  }

  /**
   * Entrance to deal with main index structure of records.
   *
   * @param curPage original page occurred overflow.
   * @param splPage new page for original page to split.
   * @param sk least key of splPage.
   */
  private void insertIndexEntryEntrance(ISchemaPage curPage, ISchemaPage splPage, String sk)
      throws MetadataException, IOException {
    if (treeTrace[0] < 1) {
      ISegmentedPage trsPage = getMinApplSegmentedPageInMem(SchemaFileConfig.SEG_MAX_SIZ);
      trsPage.transplantSegment(
          curPage.getAsSegmentedPage(), (short) 0, SchemaFileConfig.SEG_MAX_SIZ);
      ISchemaPage repPage =
          ISchemaPage.initInternalPage(
              ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH),
              curPage.getPageIndex(),
              trsPage.getPageIndex());

      if (0 > repPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex())) {
        throw new ColossalRecordException(sk);
      }

      repPage
          .getAsInternalPage()
          .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));
      replacePageInCache(repPage);
    } else {
      insertIndexEntryRecursiveUpwards(treeTrace[0], sk, splPage.getPageIndex());
    }
  }

  /**
   * Insert an index entry into an internal page. Cascade insert or internal split conducted if
   * necessary.
   *
   * @param key key of the entry
   * @param ptr pointer of the entry
   */
  private void insertIndexEntryRecursiveUpwards(int treeTraceIndex, String key, int ptr)
      throws MetadataException, IOException {
    ISchemaPage idxPage = getPageInstance(treeTrace[treeTraceIndex]);
    if (idxPage.getAsInternalPage().insertRecord(key, ptr) < 0) {
      // handle when insert an index entry occurring an overflow
      if (treeTraceIndex > 1) {
        // overflow, but existed parent to insert the split
        ByteBuffer dstBuffer = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
        String splitKey =
            idxPage
                .getAsInternalPage()
                .splitByKey(key, ptr, dstBuffer, SchemaFileConfig.INCLINED_SPLIT);
        ISchemaPage dstPage = ISchemaPage.loadSchemaPage(dstBuffer);
        registerAsNewPage(dstPage);
        insertIndexEntryRecursiveUpwards(treeTraceIndex - 1, splitKey, dstPage.getPageIndex());
      } else {
        // treeTraceIndex==1, idxPage is the root of B+Tree, to split for new root internal
        ByteBuffer splBuffer = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
        ByteBuffer trsBuffer = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);

        // idxPage shall be split, and reserved as root of B+Tree
        String splitKey =
            idxPage
                .getAsInternalPage()
                .splitByKey(key, ptr, splBuffer, SchemaFileConfig.INCLINED_SPLIT);
        idxPage.getAsInternalPage().extendsTo(trsBuffer);
        ISchemaPage splPage = ISchemaPage.loadSchemaPage(splBuffer);
        ISchemaPage trsPage = ISchemaPage.loadSchemaPage(trsBuffer);
        registerAsNewPage(splPage);
        registerAsNewPage(trsPage);

        idxPage.getAsInternalPage().resetBuffer(trsPage.getPageIndex());
        if (idxPage.getAsInternalPage().insertRecord(splitKey, splPage.getPageIndex()) < 0) {
          throw new ColossalRecordException(splitKey);
        }
        idxPage
            .getAsInternalPage()
            .setNextSegAddress(trsPage.getAsInternalPage().getNextSegAddress());
      }
    }
    markDirty(idxPage);
    addPageToCache(idxPage.getPageIndex(), idxPage);
  }

  @Override
  public void delete(IMNode node) throws IOException, MetadataException {
    // remove corresponding record
    long recSegAddr = getNodeAddress(node.getParent());
    recSegAddr = getTargetSegmentAddress(recSegAddr, node.getName());
    ISchemaPage tarPage = getPageInstance(getPageIndex(recSegAddr));
    markDirty(tarPage);
    tarPage.getAsSegmentedPage().removeRecord(getSegIndex(recSegAddr), node.getName());

    // remove segments belongs to node
    if (!node.isMeasurement()) {
      long delSegAddr = getNodeAddress(node);
      tarPage = getPageInstance(getPageIndex(delSegAddr));

      if (tarPage.getAsSegmentedPage() != null) {
        // TODO: may produce fractured page
        markDirty(tarPage);
        tarPage.getAsSegmentedPage().deleteSegment(getSegIndex(delSegAddr));
        if (tarPage.getAsSegmentedPage().validSegments() == 0) {
          tarPage.getAsSegmentedPage().purgeSegments();
        }
      }

      if (tarPage.getAsInternalPage() != null) {
        Deque<Integer> cascadePages = new ArrayDeque<>(tarPage.getAsInternalPage().getAllRecords());
        cascadePages.add(tarPage.getPageIndex());

        if (tarPage.getSubIndex() >= 0) {
          cascadePages.add(tarPage.getSubIndex());
        }

        while (!cascadePages.isEmpty()) {
          if (dirtyPages.size() > SchemaFileConfig.PAGE_CACHE_SIZE) {
            flushDirtyPages();
          }

          tarPage = getPageInstance(cascadePages.poll());
          if (tarPage.getAsSegmentedPage() != null) {
            tarPage.getAsSegmentedPage().purgeSegments();
            markDirty(tarPage);
            addPageToCache(tarPage.getPageIndex(), tarPage);
            continue;
          }

          if (tarPage.getAsInternalPage() != null) {
            cascadePages.addAll(tarPage.getAsInternalPage().getAllRecords());
          }

          tarPage =
              ISchemaPage.initSegmentedPage(
                  ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), tarPage.getPageIndex());
          replacePageInCache(tarPage);
        }
      }
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

    if (child == null && parent.isEntity()) {
      // try read alias directly first
      child =
          getPageInstance(getPageIndex(actualSegAddr))
              .getAsSegmentedPage()
              .readByAlias(getSegIndex(actualSegAddr), childName);
      if (child != null) {
        return child;
      }

      // try read with sub-index
      return getChildWithAlias(parent, childName);
    }
    return child;
  }

  private IMNode getChildWithAlias(IMNode par, String alias) throws IOException, MetadataException {
    long srtAddr = getNodeAddress(par);
    ISchemaPage page = getPageInstance(getPageIndex(srtAddr));

    if (page.getAsInternalPage() == null || page.getSubIndex() < 0) {
      return null;
    }

    String name = searchSubIndexAlias(page.getSubIndex(), alias);

    if (name == null) {
      return null;
    }

    return getTargetLeafPage(page, name).getAsSegmentedPage().read((short) 0, name);
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
        if (!children.isEmpty()) {
          return true;
        }
        if (nextSeg < 0) {
          return false;
        }

        try {
          ISchemaPage nPage;
          while (children.isEmpty() && nextSeg >= 0) {
            nPage = getPageInstance(getPageIndex(nextSeg));
            children = nPage.getAsSegmentedPage().getChildren(getSegIndex(nextSeg));
            nextSeg = nPage.getAsSegmentedPage().getNextSegAddress(getSegIndex(nextSeg));
          }
        } catch (MetadataException | IOException e) {
          logger.error(e.getMessage());
          return false;
        }

        return !children.isEmpty();
      }

      @Override
      public IMNode next() {
        return children.poll();
      }
    };
  }

  /** Seek non-InternalPage by name, syntax sugar of {@linkplain #getTargetSegmentAddress}. */
  private ISchemaPage getTargetLeafPage(ISchemaPage topPage, String recKey)
      throws IOException, MetadataException {
    treeTrace[0] = 0;
    if (topPage.getAsInternalPage() == null) {
      return topPage;
    }
    ISchemaPage curPage = topPage;

    int i = 0; // mark the trace of b+ tree node
    while (curPage.getAsInternalPage() != null) {
      i++;
      treeTrace[i] = curPage.getPageIndex();
      curPage = getPageInstance(curPage.getAsInternalPage().getRecordByKey(recKey));
    }
    treeTrace[0] = i; // bound in no.0 elem, points the parent the return

    return curPage;
  }

  /**
   * Search the segment contains target key with a B+Tree structure.
   *
   * @param curSegAddr address of the parent.
   * @param recKey target key.
   * @return address of the target segment.
   */
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
