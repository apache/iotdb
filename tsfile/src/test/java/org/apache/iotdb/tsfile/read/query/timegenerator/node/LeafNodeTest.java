package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.query.iterator.BaseJoin;
import org.apache.iotdb.tsfile.read.query.iterator.EqualJoin;
import org.apache.iotdb.tsfile.read.query.iterator.LeafNode;
import org.apache.iotdb.tsfile.read.query.iterator.SeriesIterator;
import org.apache.iotdb.tsfile.read.query.iterator.TimeSeries;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.LocalNioTsFileOutput;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
public class LeafNodeTest {

  //  public static FileSystem fs = Jimfs.newFileSystem(Configuration.unix());
  public static FileSystem fs = FileSystems.getDefault();

  @Param("1000000")
  long records;

  @Param({"false", "true"})
  boolean optimize;

  @Benchmark
  @Measurement(iterations = 3)
  @Warmup(iterations = 3)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @BenchmarkMode(Mode.AverageTime)
  @Fork(2)
  public void testSomething(Blackhole blackhole) throws IOException {
    // Set optimize flag
//    ChunkReader.optimize.set(optimize);
    // For a simple file system with Unix-style paths and behavior:
    LocalTsFileInput fileInput = new LocalTsFileInput(fs.getPath("/tmp/test.tsfile"));
    try (TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(fileInput)) {
      CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(fileSequenceReader);

      MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(fileSequenceReader);
      List<IChunkMetadata> chunkMetadataList =
          metadataQuerier.getChunkMetaDataList(new Path("d1", "s1"));

      if (!optimize) {

        IBatchReader reader = new FileSeriesReader(chunkLoader, chunkMetadataList, null);

        LeafNode leafNode = new LeafNode(reader);

        while (leafNode.hasNext()) {
          long ts = leafNode.next();
          Object value = leafNode.currentValue();
          if (blackhole != null) {
            blackhole.consume(ts);
            blackhole.consume(value);
          }
        }
      } else {

        SeriesIterator iterator = new SeriesIterator(chunkLoader, chunkMetadataList, null);

        while (iterator.hasNext()) {
          Object[] next = iterator.next();
          blackhole.consume(next);
        }
      }
    }
  }

  /**
   * Waits till all clients have gone forward
   */
  public static class SimpleFanOut {

    private final TimeSeries timeSeries;

    public SimpleFanOut(TimeSeries timeSeries) {
      this.timeSeries = timeSeries;
    }

    public TimeSeries getTimeSeries() {
      return new FanOutTimeseries(this);
    }

    public static class FanOutTimeseries implements TimeSeries {
      private final SimpleFanOut parent;

      public FanOutTimeseries(SimpleFanOut parent) {
        this.parent = parent;
      }

      @Override
      public TSDataType[] getSpecification() {
        return new TSDataType[0];
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Object[] next() {
        return new Object[0];
      }
    }
  }


  @Test
  public void joinTwoTS() throws IOException {
    setup2(100);

    LocalTsFileInput fileInput = new LocalTsFileInput(fs.getPath("/tmp/test.tsfile"));
    try (TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(fileInput)) {
      CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(fileSequenceReader);

      MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(fileSequenceReader);
      SeriesIterator left = new SeriesIterator(chunkLoader, metadataQuerier.getChunkMetaDataList(new Path("d1", "s1")), null);
      SeriesIterator right = new SeriesIterator(chunkLoader, metadataQuerier.getChunkMetaDataList(new Path("d1", "s2")), null);



      BaseJoin equalJoin = new EqualJoin(left, right);
//      BaseJoin leftJoin = new LeftJoin(left, right);
//      BaseJoin mergeJoin = new MergeJoin(left, right);

      System.out.println("Resulting Types are " + Arrays.toString(equalJoin.getSpecification()));

      while (equalJoin.hasNext()) {
        Object[] next = equalJoin.next();

        System.out.println(Arrays.toString(next));
      }
    }
  }

  public void setup2(int records) throws IOException {
    // Write a file first
    Schema schema = new Schema();
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    schema.registerTimeseries(
        new Path("d1"),
        new MeasurementSchema(
            "s1", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1"),
        new MeasurementSchema(
            "s2", TSDataType.FLOAT, TSEncoding.valueOf(conf.getValueEncoder())));

    java.nio.file.Path path = fs.getPath("/tmp");
    if (!Files.exists(path)) {
      Files.createDirectory(path);
    }
    path = fs.getPath("/tmp/test.tsfile");
    if (Files.exists(path)) {
      Files.delete(path);
    }

    Random random = new Random();
    try (TsFileWriter writer = new TsFileWriter(new LocalNioTsFileOutput(path), schema)) {
      for (long ts = 1; ts <= records; ts++) {
        TSRecord record = new TSRecord(ts, "d1");
//        if (ts % 2 == 1) {
        record.addTuple(new LongDataPoint("s1", random.nextLong()));
//        }
//        }
        if (ts % 2 == 0) {
          record.addTuple(new FloatDataPoint("s2", random.nextFloat()));
        }
        writer.write(record);
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  // @Setup(Level.Trial)
  public void setup() throws IOException {
    // Write a file first
    Schema schema = new Schema();
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    schema.registerTimeseries(
        new Path("d1"),
        new MeasurementSchema(
            "s1", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1"),
        new MeasurementSchema(
            "s2", TSDataType.FLOAT, TSEncoding.valueOf(conf.getValueEncoder())));

    java.nio.file.Path path = fs.getPath("/tmp");
    if (!Files.exists(path)) {
      Files.createDirectory(path);
    }
    path = fs.getPath("/tmp/test.tsfile");
    if (Files.exists(path)) {
      Files.delete(path);
    }

    Random random = new Random();
    try (TsFileWriter writer = new TsFileWriter(new LocalNioTsFileOutput(path), schema)) {
      for (long ts = 1; ts <= records; ts++) {
        TSRecord record = new TSRecord(ts, "d1");
        record.addTuple(new LongDataPoint("s1", random.nextLong()));
        record.addTuple(new FloatDataPoint("s2", random.nextFloat()));
        writer.write(record);
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSomething() throws IOException {
    records = 1000000;
    setup();
    testSomething(null);
  }

}