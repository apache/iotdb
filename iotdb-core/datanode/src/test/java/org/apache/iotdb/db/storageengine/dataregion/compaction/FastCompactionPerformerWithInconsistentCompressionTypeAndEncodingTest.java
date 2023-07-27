package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FastCompactionPerformerWithInconsistentCompressionTypeAndEncodingTest extends AbstractCompactionTest {

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException, InterruptedException {
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void test1() throws MetadataException, IOException, WriteProcessException {

    TsFileResource seqResource1 = generateSingleNonAlignedSeriesFile(
        "d0",
        "s0",
        new TimeRange[]{new TimeRange(100000, 200000), new TimeRange(300000, 500000)},
        TSEncoding.PLAIN,
        CompressionType.LZ4,
        true);
    seqResources.add(seqResource1);

    TsFileResource seqResource2 = generateSingleNonAlignedSeriesFile(
        "d0",
        "s0",
        new TimeRange[]{new TimeRange(600000, 700000), new TimeRange(800000, 900000)},
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TsFileResource unseqResource = generateSingleNonAlignedSeriesFile(
        "d0",
        "s0",
        new TimeRange[]{new TimeRange(210000, 290000), new TimeRange(710000, 890000)},
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        true);
    unseqResources.add(unseqResource);


    CrossSpaceCompactionTask task = new CrossSpaceCompactionTask( 0,
        tsFileManager,
        seqResources,
        unseqResources,
        new FastCompactionPerformer(true),
        new AtomicInteger(0),
        0,
        0
    );

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    validateSingleTsFile(reader);
  }


  @Test
  public void test2() throws MetadataException, IOException, WriteProcessException {

    TimeRange[] file1Chunk1 = new TimeRange[]{new TimeRange(10000, 15000), new TimeRange(16000, 19000), new TimeRange(20000, 29000)};
    TimeRange[] file1Chunk2 = new TimeRange[]{new TimeRange(30000, 35000), new TimeRange(36000, 39000), new TimeRange(40000, 49000)};

    TsFileResource seqResource1 = generateSingleNonAlignedSeriesFile(
        "d0",
        "s0",
        new TimeRange[][]{file1Chunk1, file1Chunk2},
        TSEncoding.PLAIN,
        CompressionType.LZ4,
        true);
    seqResources.add(seqResource1);

    TimeRange[] file2Chunk1 = new TimeRange[]{new TimeRange(50000, 55000), new TimeRange(56000, 59000), new TimeRange(60000, 69000)};
    TimeRange[] file2Chunk2 = new TimeRange[]{new TimeRange(70000, 75000), new TimeRange(76000, 79000), new TimeRange(180000, 189000)};
    TsFileResource seqResource2 = generateSingleNonAlignedSeriesFile(
        "d0",
        "s0",
        new TimeRange[][]{file2Chunk1, file2Chunk2},
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        true);
    seqResources.add(seqResource2);

    tsFileManager.addAll(seqResources, true);

    TimeRange[] unseqFileChunk1 = new TimeRange[]{new TimeRange(1000, 5000), new TimeRange(96000, 99000), new TimeRange(100000, 110000)};
    TimeRange[] unseqFileChunk2 = new TimeRange[]{new TimeRange(120000, 130000), new TimeRange(136000, 149000), new TimeRange(200000, 210000)};
    TsFileResource unseqResource = generateSingleNonAlignedSeriesFile(
        "d0",
        "s0",
        new TimeRange[][]{unseqFileChunk1, unseqFileChunk2},
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        true);
    unseqResources.add(unseqResource);


    CrossSpaceCompactionTask task = new CrossSpaceCompactionTask( 0,
        tsFileManager,
        seqResources,
        unseqResources,
        new FastCompactionPerformer(true),
        new AtomicInteger(0),
        0,
        0
    );

    Assert.assertTrue(task.start());
    TsFileResource targetFile = tsFileManager.getTsFileList(true).get(0);
    TsFileSequenceReader reader = new TsFileSequenceReader(targetFile.getTsFilePath());
    validateSingleTsFile(reader);
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(String device, String measurement, TimeRange[] chunkTimeRanges, TSEncoding encoding, CompressionType compressionType, boolean isSeq) throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement,
        chunkTimeRanges,
        encoding,
        compressionType
    );
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private TsFileResource generateSingleNonAlignedSeriesFile(String device, String measurement, TimeRange[][] chunkTimeRanges, TSEncoding encoding, CompressionType compressionType, boolean isSeq) throws IOException {
    TsFileResource seqResource1 = createEmptyFileAndResource(isSeq);
    CompactionTestFileWriter writer1 = new CompactionTestFileWriter(seqResource1);
    writer1.startChunkGroup(device);
    writer1.generateSimpleNonAlignedSeriesToCurrentDevice(
        measurement,
        chunkTimeRanges,
        encoding,
        compressionType
    );
    writer1.endChunkGroup();
    writer1.endFile();
    writer1.close();
    return seqResource1;
  }

  private boolean validateSingleTsFile(TsFileSequenceReader reader) throws IOException {
    for (String device : reader.getAllDevices()) {
      Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
      for (Map.Entry<String, List<ChunkMetadata>> entry : seriesMetaData.entrySet()) {
        String series = entry.getKey();
        List<ChunkMetadata> chunkMetadataList = entry.getValue();
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          ChunkReader chunkReader = new ChunkReader(chunk);
          ChunkHeader chunkHeader = chunk.getHeader();
          ByteBuffer chunkDataBuffer = chunk.getData();
          System.out.println(chunkHeader.getCompressionType());
          while (chunkDataBuffer.remaining() > 0) {
            PageHeader pageHeader;
            if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
            } else {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
            }
            chunkReader.readPageData(pageHeader, chunkReader.readPageDataWithoutUncompressing(pageHeader));
//            IUnCompressor.getUnCompressor(CompressionType.SNAPPY).uncompress(chunkReader.readPageDataWithoutUncompressing(pageHeader).array());
          }
        }
      }
    }
    return true;
  }

}
