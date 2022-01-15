package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.SchemaPageOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


/**
 * TODO: Specify durability design
 *
 * This class is mainly aimed to manage space all over the file.
 *
 * This class is meant to open a .pmt(Persistent MTree) file, and maintains the header of the file.
 * It Loads or writes a page length bytes at once, with an 32 bits int to index a page inside a file.
 * Use SlottedFile to manipulate segment(sp) inside a page(an array of bytes).
 * */
public class SchemaFile implements ISchemaFile{

  private static final Logger logger = LoggerFactory.getLogger(SchemaFile.class);

  // attributes for this schema file
  String filePath;
  String storageGroupName;

  ByteBuffer headerContent;
  int lastPageIndex;  // last page index of the file, boundary to grow

  // work as a naive cache for page instance
  Map<Integer, ISchemaPage> pageInstCache;
  ISchemaPage rootPage;

  // attributes inside current page

  FileChannel channel;

  @TestOnly
  public SchemaFile() throws IOException, MetadataException {
    this("testPMT");
  }

  public SchemaFile(String sgName) throws IOException, MetadataException {
    this.storageGroupName = sgName;
    filePath = SchemaFile.FILE_FOLDER + sgName + ".pmt";

    pageInstCache = new HashMap<>();

    File pmtFile = new File(filePath);
    if (!pmtFile.exists() || !pmtFile.isFile()) {
      File folder = new File(SchemaFile.FILE_FOLDER);
      folder.mkdirs();
      pmtFile.createNewFile();
    }

    channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFile.FILE_HEADER_SIZE);
    initFileHeader();
  }

  // region Interface Implementation

  @Override
  public void write(IMNode node) throws MetadataException, IOException {
    int pageIndex;
    short curSegIdx;
    ISchemaPage curPage;

    // Get corresponding page instance, segment id
    long curSegAddr = node.getChildren().getSegment().getSegmentAddress();
    if (curSegAddr < 0) {
      if (node.getParent() == null) {
        // root node
        curPage = getRootPage();
        pageIndex = curPage.getPageIndex();
        curSegIdx = 0;
      } else {
        throw new MetadataException("Cannot store a node without segment address except for root.");
      }
    } else {
      pageIndex = ISchemaFile.getPageIndex(curSegAddr);
      curSegIdx = ISchemaFile.getSegIndex(curSegAddr);
      curPage = getPageInstance(pageIndex);
    }

    // Translate node.child into buffer and Pre-Allocate segment for internal child.
    for(Map.Entry<String, IMNode> entry:
        node.getChildren().getSegment().getNewChildBuffer().entrySet()) {
      IMNode child = entry.getValue();
      if (!child.isMeasurement()) {
        if (child.getChildren().getSegment().getSegmentAddress() < 0) {
          short estSegSize = estimateSegmentSize(child);
          long glbIndex = preAllocateSegment(estSegSize);
          child.getChildren().getSegment().setSegmentAddress(glbIndex);
        } else {
          // new child with a valid segment address, weird
          throw new MetadataException("A child in newChildBuffer shall not have segmentAddress.");
        }
      }

      ByteBuffer childBuffer = RecordUtils.node2Buffer(child);

      // Write and Handle Overflow
      // (inside curPage.write)
      // write to curSeg, return 0 if succeeded, positive if next page, exception if no next and no spare
      // (inside this method)
      // get next segment if existed, retry till succeeded or exception
      // allocate new page if exception, link or transplant by original segment size, write buffer to new segment
      // throw exception if record larger than one page
      try {
        long npAddress = curPage.write(curSegIdx, entry.getKey(), childBuffer);
        while (npAddress > 0) {
          // get next page and retry
          pageIndex = ISchemaFile.getPageIndex(npAddress);
          curSegIdx = ISchemaFile.getSegIndex(npAddress);
          curPage = getPageInstance(pageIndex);
          npAddress = curPage.write(curSegIdx, entry.getKey(), childBuffer);
        }

      } catch (SchemaPageOverflowException e) {
        // there is no more next page, need allocate new page
        short newSegSize = ISchemaFile.reEstimateSegSize(curPage.getSegmentSize(curSegIdx));
        ISchemaPage newPage = getMinApplicablePageInMem(newSegSize);

        if (newSegSize == curPage.getSegmentSize(curSegIdx)) {
          // segment on multi pages
          short newSegId = newPage.allocNewSegment(newSegSize);
          long newSegAddr = ISchemaFile.getGlobalIndex(newPage.getPageIndex(), newSegId);

          // note that it doesn't modify address of node nor parental record
          newPage.setPrevSegAddress(newSegId, curSegAddr);
          curPage.setNextSegAddress(curSegIdx, newSegAddr);

          curSegAddr = newSegAddr;
        } else {
          // segment on single page
          curSegAddr = newPage.transplantSegment(curPage, curSegIdx, newSegSize);
          if (curSegAddr < 0) {
            throw new MetadataException("Transplant segment filed for unknown reason.");
          }

          curPage.deleteSegment(curSegIdx);

          curSegIdx = ISchemaFile.getSegIndex(curSegAddr);
        }
        curPage = newPage;
        curPage.write(curSegIdx, entry.getKey(), childBuffer);
      }
    }

    // handle update child
    for(Map.Entry<String, IMNode> entry:
        node.getChildren().getSegment().getUpdatedChildBuffer().entrySet()) {

    }
  }

  private ISchemaPage getRootPage() {
    return rootPage;
  }

  private short estimateSegmentSize(IMNode node) {
    return (short) ( ISchemaFile.SEG_SIZE_LST[0] / 2 );
  }

  private long preAllocateSegment(short size) throws IOException, MetadataException {
    ISchemaPage page = getMinApplicablePageInMem(size);
    return ISchemaFile.getGlobalIndex(page.getPageIndex(), page.allocNewSegment(size));
  }

  @Override
  public void write(Collection<IMNode> nodes) {

  }

  @Override
  public void delete(IMNode node) {

  }

  @Override
  public IMNode read(PartialPath path) {
    return null;
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) {
    return null;
  }

  @Override
  public void close() throws IOException {
    updateHeader();
    for (Map.Entry<Integer, ISchemaPage> entry: pageInstCache.entrySet()) {
      flushPageToFile(entry.getValue());
    }
    channel.close();
  }

  @Override
  public String inspect() throws MetadataException, IOException {
    StringBuilder builder = new StringBuilder(String.format(
        "SchemaFile inspect: %s, totalPages:%d\n", storageGroupName, lastPageIndex));
    int cnt = 0;
    while (cnt <= lastPageIndex) {
      ISchemaPage page = getPageInstance(cnt);
      builder.append(String.format("----------\n%s\n", page.inspect()));
      cnt ++;
    }
    return builder.toString();
  }
  // endregion

  // region File Operations

  /**
   * File Header Structure:
   * 1 int (4 bytes): last page index
   * var length: root(SG) node info
   *    a. var length string (less than 200 bytes): path to root(SG) node
   *    b. fixed length buffer (13 bytes): internal or entity node buffer
   * */
  private void initFileHeader() throws IOException, MetadataException {
    if (channel.size() == 0) {
      // new schema file
      lastPageIndex = 0;
      ReadWriteIOUtils.write(lastPageIndex, headerContent);
      ReadWriteIOUtils.write(storageGroupName, headerContent);
      initRootPage();
    } else {
      channel.read(headerContent);
      headerContent.clear();
      lastPageIndex = ReadWriteIOUtils.readInt(headerContent);
      storageGroupName = ReadWriteIOUtils.readString(headerContent);
      rootPage = getPageInstance(0);
    }
  }

  private void updateHeader() throws IOException{
    headerContent.clear();

    ReadWriteIOUtils.write(lastPageIndex, headerContent);
    ReadWriteIOUtils.write(storageGroupName, headerContent);

    headerContent.clear();
    channel.write(headerContent, 0);
    channel.force(true);
  }

  private void initRootPage() throws IOException, MetadataException{
    if (rootPage == null) {
      rootPage = SchemaPage.initPage(ByteBuffer.allocate(PAGE_LENGTH), 0);
      rootPage.allocNewSegment(SEG_MAX_SIZ);

      lastPageIndex = 0;

      pageInstCache.put(rootPage.getPageIndex(), rootPage);
    }
  }

  // endregion

  // region space management


  /**
   * This method checks with cached page container, get a minimum applicable page for allocation.
   * @param size
   * @return
   */
  private ISchemaPage getMinApplicablePageInMem(short size) {
    for (Map.Entry<Integer, ISchemaPage> entry: pageInstCache.entrySet()) {
      if (entry.getValue().isCapableForSize(size)) {
        return entry.getValue();
      }
    }
    return allocateNewPage();
  }

  /**
   * Get from cache, or load from file.
   * TODO: improve with file-wide page cache
   * @param pageIdx target page index
   * @return an existed page
   */
  private ISchemaPage getPageInstance(int pageIdx) throws IOException, MetadataException {
    if (pageIdx > lastPageIndex) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    if (pageInstCache.containsKey(pageIdx)) {
      return pageInstCache.get(pageIdx);
    }

    ByteBuffer newBuf = ByteBuffer.allocate(PAGE_LENGTH);

    loadFromFile(newBuf, pageIdx);
    return SchemaPage.loadPage(newBuf, pageIdx);
  }

  private ISchemaPage allocateNewPage() {
    lastPageIndex += 1;
    ISchemaPage newPage = SchemaPage.initPage(ByteBuffer.allocate(PAGE_LENGTH), lastPageIndex);

    pageInstCache.put(newPage.getPageIndex(), newPage);

    return newPage;
  }

  // endregion


  // region utils

  private long getPageAddress(int pageIndex) {
    return pageIndex * PAGE_LENGTH + FILE_HEADER_SIZE;
  }

  private int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException{
    dst.clear();
    return channel.read(dst, getPageAddress(pageIndex));
  }

  private void flushPageToFile(ISchemaPage src) throws IOException{
    src.syncPageBuffer();

    ByteBuffer srcBuf = ByteBuffer.allocate(ISchemaFile.PAGE_LENGTH);
    src.getPageBuffer(srcBuf);
    srcBuf.clear();
    channel.write(srcBuf, getPageAddress(src.getPageIndex()));
  }

  // endregion
}
