package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;


/**
 * TODO: Specify durability design
 * This class is meant to open a .pmt(Persistent MTree) file, and maintains the header of the file.
 * It Loads or writes a page length bytes at once, with an 32 bits int to index a page inside a file.
 * Use SlottedFile to manipulate segment(sp) inside a page(an array of bytes).
 * */
public class SchemaFile implements ISchemaFile{

  private static final Logger logger = LoggerFactory.getLogger(SchemaFile.class);

  static final int PAGE_LENGTH = 16 * 1024;  // 16 kib for default
  static final int INDEX_LENGTH = 4;  // 32 bit for page pointer, maximum .pmt file as 2^(32+14) bytes, 64 TiB
  static final int HEADER_INDEX = 0;  // index of header page
  static final int PMT_HEADER_SIZE = 256;  // size of file header in bytes
  static final int PAGE_HEADER_SIZE = 256;
  static final String PMT_FOLDER = "./pst/";  // folder to store .pmt files

  // attributes inside this schema file
  String filePath;
  String storageGroupName;

  ByteBuffer headerContent, currentPage;
  int currentPageIndex, lastPageIndex;

  // attributes inside current page
  boolean pageDelFlag;
  List<Short> segAddress;  // segment address array inside a page
  short freeAddressInPage;


  FileChannel channel;

  @TestOnly
  public SchemaFile() throws IOException {
    this("testPMT");
  }

  public SchemaFile(String sgName) throws IOException {
    this.storageGroupName = sgName;
    filePath = SchemaFile.PMT_FOLDER + sgName + ".pmt";

    File pmtFile = new File(filePath);
    if (!pmtFile.exists() || !pmtFile.isFile()) {
      File folder = new File(SchemaFile.PMT_FOLDER);
      folder.mkdirs();
      pmtFile.createNewFile();
    }

    channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFile.PMT_HEADER_SIZE);
    currentPage = ByteBuffer.allocate(SchemaFile.PAGE_LENGTH);
    initFileHeader();
  }

  // region interface implementation
  /**
   * Check whether root node for this file (for now, it is pstAddress == -1)
   *
   * */
  @Override
  public void write(IMNode node) {
    boolean isRootNode = true;
    // root node
    if (isRootNode) {

    } else {

    }

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
    channel.close();
  }
  // endregion

  // region file decoder

  /**
   * File Header:
   * int: last page index
   * root node info
   *  a. var length string: name
   *  (b. boolean: alignment)
   * */
  private void initFileHeader() throws IOException {
    if (channel.size() == 0) {
      // new schema file
      currentPageIndex = 0;
      lastPageIndex = 0;
      ReadWriteIOUtils.write(lastPageIndex, headerContent);
      ReadWriteIOUtils.write(storageGroupName, headerContent);
      updateHeader(0L);

      initPageHeader();
      segAddress.add(freeAddressInPage);
      // init this segment as root segment

    } else {
      channel.read(headerContent);
      headerContent.position(0);
      lastPageIndex = ReadWriteIOUtils.readInt(headerContent);
      storageGroupName = ReadWriteIOUtils.readString(headerContent);
    }
  }

  private void updateHeader(long position) throws IOException{
    headerContent.position(0);
    channel.write(headerContent, position);
    channel.force(true);
  }

  // endregion
  // region page decoder

  /**
   * Page Header Structure:
   * boolean: delete flag
   * var length short list: segment address
   * short: free space index
   * (short: last delete segment address)
   * */
  private void initPageHeader() {
    pageDelFlag = false;
    segAddress = new ArrayList<>();
    freeAddressInPage = SchemaFile.PAGE_HEADER_SIZE;
  }

  private ByteBuffer getSegmentBuffer(int index) {
    short segAddr = segAddress.get(index);
    currentPage.position(segAddr);
    short length = ReadWriteIOUtils.readShort(currentPage);
    currentPage.position(segAddr);
    currentPage.limit(segAddr + length);
    return currentPage.slice();
  }

  // endregion

  // region space management

  private void allocateNewPage() throws IOException{
    // TODO: improve rather than append way
    loadPage(++currentPageIndex);
    initPageHeader();
  }

  /**
   * Try to allocate segment within current page
   * Will allocate new page if no available segment
   * Record segment address into segAddress
   * @param size target size of the segment
   * @return concatenation of PAGE_INDEX and SEG_INDEX
   * @throws IOException
   */
  private long allocateSegment(short size) throws IOException {
    return 0L;
  }

  // endregion


  // region utils

  private long getPageAddress(int pageIndex) {
    return pageIndex * PAGE_LENGTH + PMT_HEADER_SIZE;
  }

  private void loadPage(int pageIndex) throws IOException {
    writePage2File();
    currentPage.clear();
    currentPageIndex = pageIndex;
    channel.read(currentPage, getPageAddress(currentPageIndex));
  }

  private void writePage2File() throws IOException{
    channel.write(currentPage, getPageAddress(currentPageIndex));
  }

  // endregion
}
