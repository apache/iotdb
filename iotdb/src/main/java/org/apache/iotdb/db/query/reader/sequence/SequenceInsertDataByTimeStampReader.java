//package org.apache.iotdb.read.reader;
//
//import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
//import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
//import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
//import org.apache.iotdb.db.query.control.FileReaderManager;
//import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
//import org.apache.iotdb.db.query.reader.merge.PrioritySeriesReader;
//import org.apache.iotdb.db.query.reader.merge.PrioritySeriesReaderByTimestamp;
//import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
//import org.apache.iotdb.db.query.reader.mem.MemChunkReaderByTimestamp;
//import org.apache.iotdb.db.utils.TimeValuePair;
//import org.apache.iotdb.db.utils.TsPrimitiveType;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * A reader for sequence insert data which can get the corresponding value of the specified time point.
// */
//public class SequenceInsertDataByTimeStampReader extends SequenceDataReader implements EngineReaderByTimeStamp {
//
//  private long currentTimestamp;
//  private PriorityMergeReaderByTimestamp priorityMergeSortTimeValuePairReader;
//
//  public SequenceInsertDataByTimeStampReader(GlobalSortedSeriesDataSource sortedSeriesDataSource)
//          throws IOException {
//    super(sortedSeriesDataSource);
//
//    List<PrioritySeriesReaderByTimestamp> priorityTimeValuePairReaderByTimestamps = new ArrayList<>();
//    int priority = 1;
//
//    //data in sealedTsFiles and unSealedTsFile
//    if (sortedSeriesDataSource.getSealedTsFiles() != null) {
//      SealedTsFileWithTimeStampReader sealedTsFileWithTimeStampReader = new SealedTsFileWithTimeStampReader(sortedSeriesDataSource.getSealedTsFiles());
//      priorityTimeValuePairReaderByTimestamps.add(new PrioritySeriesReaderByTimestamp(sealedTsFileWithTimeStampReader, new PrioritySeriesReader.Priority(priority++)));
//    }
//    if (sortedSeriesDataSource.getUnsealedTsFile() != null) {
//      UnSealedTsFileWithTimeStampReader unSealedTsFileWithTimeStampReader = new UnSealedTsFileWithTimeStampReader(sortedSeriesDataSource.getUnsealedTsFile());
//      priorityTimeValuePairReaderByTimestamps.add(new PrioritySeriesReaderByTimestamp(unSealedTsFileWithTimeStampReader, new PrioritySeriesReader.Priority(priority++)));
//    }
//    //data in memTable
//    if (sortedSeriesDataSource.hasRawChunk()) {
//      MemChunkReaderByTimestamp rawSeriesChunkReaderByTimestamp = new MemChunkReaderByTimestamp(sortedSeriesDataSource.getReadableMemChunk());
//      priorityTimeValuePairReaderByTimestamps.add(new PrioritySeriesReaderByTimestamp(rawSeriesChunkReaderByTimestamp, new PrioritySeriesReader.Priority(priority++)));
//    }
//
//    priorityMergeSortTimeValuePairReader = new PriorityMergeReaderByTimestamp(priorityTimeValuePairReaderByTimestamps);
//    currentTimestamp = Long.MIN_VALUE;
//  }
//
//  @Override
//  public boolean hasNext() throws IOException {
//    return priorityMergeSortTimeValuePairReader.hasNext();
//  }
//
//  @Override
//  public TimeValuePair next() throws IOException {
//    return priorityMergeSortTimeValuePairReader.next();
//  }
//
//  /**
//   * @param timestamp
//   * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
//   * @throws IOException
//   */
//  @Override
//  public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
//    setCurrentTimestamp(timestamp);
//    return priorityMergeSortTimeValuePairReader.getValueInTimestamp(timestamp);
//  }
//
//
//  public void setCurrentTimestamp(long currentTimestamp) {
//    this.currentTimestamp = currentTimestamp;
//  }
//
//  private class SealedTsFileWithTimeStampReader extends SequenceDataReader.SealedTsFilesReader implements EngineReaderByTimeStamp {
//
//    private boolean hasCacheLastTimeValuePair;
//    private TimeValuePair cachedTimeValuePair;
//
//    private SealedTsFileWithTimeStampReader(List<IntervalFileNode> sealedTsFiles) {
//      super(sealedTsFiles);
//      hasCacheLastTimeValuePair = false;
//    }
//
//    @Override
//    public boolean hasNext() throws IOException {
//      //hasCached
//      if (hasCacheLastTimeValuePair && cachedTimeValuePair.getTimestamp() >= currentTimestamp) {
//        return true;
//      }
//      //tsFileReader has initialized
//      if (singleTsFileReaderInitialized) {
//        TsPrimitiveType value = ((SeriesReaderFromSingleFileByTimestampImpl) tsFileReader).getValueInTimestamp(currentTimestamp);
//        if (value != null) {
//          hasCacheLastTimeValuePair = true;
//          cachedTimeValuePair = new TimeValuePair(currentTimestamp, value);
//          return true;
//        } else {
//          if (tsFileReader.hasNext()) {
//            return true;
//          } else {
//            singleTsFileReaderInitialized = false;
//          }
//        }
//      }
//
//      while ((usedIntervalFileIndex + 1) < sealedTsFiles.size()) {
//        if (!singleTsFileReaderInitialized) {
//          IntervalFileNode fileNode = sealedTsFiles.get(++usedIntervalFileIndex);
//          //currentTimestamp<=maxTimestamp
//          if (singleTsFileSatisfied(fileNode)) {
//            initSingleTsFileReader(fileNode);
//            singleTsFileReaderInitialized = true;
//          } else {
//            continue;
//          }
//        }
//        //tsFileReader has already initialized
//        TsPrimitiveType value = ((SeriesReaderFromSingleFileByTimestampImpl) tsFileReader).getValueInTimestamp(currentTimestamp);
//        if (value != null) {
//          hasCacheLastTimeValuePair = true;
//          cachedTimeValuePair = new TimeValuePair(currentTimestamp, value);
//          return true;
//        } else {
//          if (tsFileReader.hasNext()) {
//            return true;
//          } else {
//            singleTsFileReaderInitialized = false;
//          }
//        }
//      }
//      return false;
//    }
//
//    @Override
//    public TimeValuePair next() throws IOException {
//      if (hasCacheLastTimeValuePair) {
//        hasCacheLastTimeValuePair = false;
//        return cachedTimeValuePair;
//      } else {
//        return tsFileReader.next();
//      }
//    }
//
//    protected boolean singleTsFileSatisfied(IntervalFileNode fileNode) {
//
//      if (fileNode.getStartTime(seriesPath.getDevice()) == -1) {
//        return false;
//      }
//      long maxTime = fileNode.getEndTime(seriesPath.getDevice());
//      return currentTimestamp <= maxTime;
//    }
//
//    protected void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException {
//      RandomAccessFile raf = FileReaderManager.getInstance().get(jobId, fileNode.getFilePath());
//      ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(raf);
//      tsFileReader = new SeriesReaderFromSingleFileByTimestampImpl(randomAccessFileReader, seriesPath);
//    }
//
//    @Override
//    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
//      if (hasNext()) {
//        cachedTimeValuePair = next();
//        if (cachedTimeValuePair.getTimestamp() == timestamp) {
//          return cachedTimeValuePair.getValue();
//        } else {
//          hasCacheLastTimeValuePair = true;
//        }
//      }
//      return null;
//    }
//  }
//
//  protected class UnSealedTsFileWithTimeStampReader extends SequenceDataReader.UnSealedTsFileReader implements EngineReaderByTimeStamp {
//
//    public UnSealedTsFileWithTimeStampReader(UnsealedTsFile unsealedTsFile) throws IOException {
//      super(unsealedTsFile);
//    }
//
//    @Override
//    public boolean hasNext() throws IOException {
//      return tsFileReader.hasNext();
//    }
//
//    @Override
//    public TimeValuePair next() throws IOException {
//      return tsFileReader.next();
//    }
//
//    protected void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader,
//                                          SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
//      tsFileReader = new SeriesReaderFromSingleFileByTimestampImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
//    }
//
//    @Override
//    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
//      return ((EngineReaderByTimeStamp) tsFileReader).getValueInTimestamp(timestamp);
//    }
//  }
//
//}
