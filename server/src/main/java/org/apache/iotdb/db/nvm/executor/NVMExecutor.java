//package org.apache.iotdb.db.nvm.executor;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.channels.FileChannel;
//import java.nio.channels.FileChannel.MapMode;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import org.apache.iotdb.tsfile.utils.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class NVMExecutor implements INVMExecutor {
//
//  private static final Logger logger = LoggerFactory.getLogger(NVMExecutor.class);
//
//  // 逻辑PageID -> PCM中对应脏页及每个脏页对应的脏record
//  private Map<Long, Pair<Long, Set<Long>>> mappingTable;
//
//  private final long FREE_SLOT_BITMAP_BUF_SIZE = 1000;
//  private NVMSpace freeSlotBitmapBuf;
//
//  private final long ACTIVE_TX_LIST_BUF_SIZE = 1000;
//  private NVMSpace activeTxListBuf;
//
//  private final long DATA_BUF_SIZE = 1000;
//  private NVMSpace dataBuf;
//
//  // 未提交的事务XID -> 所有脏record
//  private Map<Long, Set<Long>> transactionTable;
//  // 被未提交事务修改的page PID -> 已提交版本page及对应record的已提交版本
//  private Map<Long, Pair<Long, Set<Long>>> dirtyPageTable;
//  // 每个page PID -> DRAM中的脏record列表
//  private Map<Long, List<Long>> dirtyRecordTable;
//
//  private NVMExecutor() {
//    initMappingTable();
//
//    initNVMFields();
//
//    transactionTable = new HashMap<>();
//    dirtyPageTable = new HashMap<>();
//    dirtyRecordTable = new HashMap<>();
//  }
//
//  private static final NVMExecutor INSTANCE = new NVMExecutor();
//  public static NVMExecutor getInstance() {
//    return INSTANCE;
//  }
//
//  /**
//   * init by inverse table
//   */
//  private void initMappingTable() {
//    // TODO
//  }
//
//  private void initNVMFields() {
//    FileChannel fc = null;
//    try {
//      fc = new RandomAccessFile(NVM_PATH, "rw").getChannel();
//
//      freeSlotBitmapBuf = fc.map(MapMode.READ_WRITE, FREE_SLOT_BITMAP_BUF_OFFSET, FREE_SLOT_BITMAP_BUF_OFFSET);
//      activeTxListBuf = fc.map(MapMode.READ_WRITE, ACTIVE_TX_LIST_BUF_OFFSET, ACTIVE_TX_LIST_BUF_SIZE);
//      dataBuf = fc.map(MapMode.READ_WRITE, DATA_BUF_OFFSET, DATA_BUF_SIZE);
//    } catch (IOException e) {
//      logger.error("Fail to init NVM fields at {}.", NVM_PATH, e);
//    }
//  }
//
//  @Override
//  public void flushRecordToNVM(Record record) {
//    /*
//    Let T be the transaction that made the last update to t
//    if t is T’s first dirty record to be flushed then
//      Append T to the ActiveTxList in PCM
//    Write t to a free space of PCM
//    if there is a previously committed version of t in PCM then
//      Add the previous version to the Dirty Page Table
//    else if there is a copy of t (updated by T) in PCM then
//      Invalidate the uncommitted copy
//    Update the Mapping Table and Transaction Table */
//
//    if (isFirstToNVM(record.getXID())) {
//      addActiveTx(record.getXID());
//    }
//
//    writeRecord(record);
//
//    Record committedRecord = getPreviousComittedRecord(record);
//    if (committedRecord != null) {
//      if (!dirtyPageTable.containsKey(record.getPID())) {
//        dirtyPageTable.put(record.getPID(), new Pair<>(committedRecord.getPID(), new HashSet<>()));
//      }
//      dirtyPageTable.get(record.getPID()).right.add(record.getRID());
//    }
//  }
//
//  private Record getPreviousComittedRecord(Record record) {
//    // TODO
//    return null;
//  }
//
//  private void writeRecord(Record record) {
//    // TODO
//  }
//
//  private void addActiveTx(long XID) {
//    // TODO
//  }
//
//  private boolean isFirstToNVM(long XID) {
//    // TODO
//    return true;
//  }
//
//  @Override
//  public void commit(Transaction transaction) {
//
//  }
//
//  @Override
//  public void abort(Transaction transaction) {
//
//  }
//
//  @Override
//  public void recovery() {
//
//  }
//}
