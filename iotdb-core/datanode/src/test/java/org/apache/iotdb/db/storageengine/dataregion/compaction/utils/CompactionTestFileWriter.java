package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CompactionTestFileWriter {

  private TsFileResource resource;
  private TsFileIOWriter fileWriter;
  private static final String SG_NAME = "root.testsg";
  private String currentDeviceId;
  public CompactionTestFileWriter(TsFileResource emptyFile) throws IOException {
    this.resource = emptyFile;
    fileWriter = new TsFileIOWriter(emptyFile.getTsFile());
  }

  public String startChunkGroup(String deviceName) throws IOException {
    currentDeviceId = SG_NAME + "." + deviceName;
    fileWriter.startChunkGroup(currentDeviceId);
    return currentDeviceId;
  }

  public void endChunkGroup() throws IOException {
    fileWriter.endChunkGroup();
  }

  public void endFile() throws IOException {
    fileWriter.endFile();
  }

  public void close() throws IOException {
    fileWriter.close();
  }

  public void generateSimpleNonAlignedSeriesToCurrentDevice(String measurementName, TimeRange[] toGenerateChunkTimeRanges, TSEncoding encoding, CompressionType compressionType) throws IOException {
    MeasurementSchema schema = new MeasurementSchema(
        measurementName,
        TSDataType.INT32,
        encoding,
        compressionType
    );
    for (TimeRange timeRange : toGenerateChunkTimeRanges) {
      ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
      for (long time = timeRange.getMin(); time <= timeRange.getMax(); time++) {
        chunkWriter.write(time, new Random().nextInt());
      }
      chunkWriter.sealCurrentPage();
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleNonAlignedSeriesToCurrentDevice(String measurementName, TimeRange[][] toGenerateChunkPageTimeRanges, TSEncoding encoding, CompressionType compressionType) throws IOException {
    MeasurementSchema schema = new MeasurementSchema(
        measurementName,
        TSDataType.INT32,
        encoding,
        compressionType
    );
    for (TimeRange[] toGenerateChunk : toGenerateChunkPageTimeRanges) {
      ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
      for (TimeRange toGeneratePage : toGenerateChunk) {
        PageWriter pageWriter = chunkWriter.getPageWriter();
        for (long time = toGeneratePage.getMin(); time <= toGeneratePage.getMax(); time++) {
          pageWriter.write(time, new Random().nextInt());
        }
        chunkWriter.sealCurrentPage();
      }
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleNonAlignedSeriesToCurrentDevice(String measurementName, TimeRange[][][] toGenerateChunkPagePointsTimeRanges, TSEncoding encoding, CompressionType compressionType) throws IOException {
    MeasurementSchema schema = new MeasurementSchema(
        measurementName,
        TSDataType.INT32,
        encoding,
        compressionType
    );
    for (TimeRange[][] toGenerateChunk : toGenerateChunkPagePointsTimeRanges) {
      ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
      for (TimeRange[] toGeneratePage : toGenerateChunk) {
        PageWriter pageWriter = chunkWriter.getPageWriter();
        for (TimeRange pagePointTimeRange : toGeneratePage) {
          for (long time = pagePointTimeRange.getMin(); time <= pagePointTimeRange.getMax(); time++) {
            pageWriter.write(time, new Random().nextInt());
          }
        }
        chunkWriter.sealCurrentPage();
      }
      chunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(List<String> measurementNames, TimeRange[] toGenerateChunkTimeRanges, TSEncoding encoding, CompressionType compressionType) throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(new MeasurementSchema(
          measurementName,
          TSDataType.INT32,
          encoding,
          compressionType
      ));
    }
    for (TimeRange toGenerateChunk : toGenerateChunkTimeRanges) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
      for (long time = toGenerateChunk.getMin(); time <= toGenerateChunk.getMax(); time++) {
        alignedChunkWriter.getTimeChunkWriter().write(time);
        for (int i = 0; i < measurementNames.size(); i++) {
          alignedChunkWriter.getValueChunkWriterByIndex(i).write(time, new Random().nextInt(), false);
        }
      }
      alignedChunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(List<String> measurementNames, TimeRange[][] toGenerateChunkPageTimeRanges, TSEncoding encoding, CompressionType compressionType) throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(new MeasurementSchema(
          measurementName,
          TSDataType.INT32,
          encoding,
          compressionType
      ));
    }
    for (TimeRange[] toGenerateChunk : toGenerateChunkPageTimeRanges) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
      for (TimeRange toGeneratePageTimeRange : toGenerateChunk) {
        for (long time = toGeneratePageTimeRange.getMin(); time <= toGeneratePageTimeRange.getMax(); time++) {
          for (int i = 0; i < measurementNames.size(); i++) {
            alignedChunkWriter.getValueChunkWriterByIndex(i).getPageWriter().write(time, new Random().nextInt(), false);
          }
        }
        alignedChunkWriter.sealCurrentPage();
      }
      alignedChunkWriter.writeToFileWriter(fileWriter);
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(List<String> measurementNames, TimeRange[][][] toGenerateChunkPageTimeRanges, TSEncoding encoding, CompressionType compressionType) throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(new MeasurementSchema(
          measurementName,
          TSDataType.INT32,
          encoding,
          compressionType
      ));
    }
    for (TimeRange[][] toGenerateChunk : toGenerateChunkPageTimeRanges) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
      for (TimeRange[] toGeneratePageTimeRanges : toGenerateChunk) {
        for (TimeRange pointsTimeRange : toGeneratePageTimeRanges) {
          for (long time = pointsTimeRange.getMin(); time <= pointsTimeRange.getMax(); time++) {
            for (int i = 0; i < measurementNames.size(); i++) {
              alignedChunkWriter.getValueChunkWriterByIndex(i).getPageWriter().write(time, new Random().nextInt(), false);
            }
          }
        }
        alignedChunkWriter.sealCurrentPage();
      }
      alignedChunkWriter.writeToFileWriter(fileWriter);
    }
  }
}
