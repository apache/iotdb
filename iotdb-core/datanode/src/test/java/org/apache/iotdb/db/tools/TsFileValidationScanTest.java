package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.tools.utils.TsFileValidationScan;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TsFileValidationScanTest {

  public static void main(String[] args) throws IOException {}

  @Test
  public void testValidation() throws IOException {
    List<File> files = prepareTsFiles();
    try {
      // overlap between chunks
      TsFileValidationScan tsFileValidationScan = new TsFileValidationScan(null);
      tsFileValidationScan.scanTsFile(files.get(0));
      assertEquals(1, tsFileValidationScan.getBadFileNum());

      // overlap between page
      tsFileValidationScan = new TsFileValidationScan(null);
      tsFileValidationScan.scanTsFile(files.get(1));
      assertEquals(1, tsFileValidationScan.getBadFileNum());

      // overlap within page
      tsFileValidationScan = new TsFileValidationScan(null);
      tsFileValidationScan.scanTsFile(files.get(2));
      assertEquals(1, tsFileValidationScan.getBadFileNum());

      // normal
      tsFileValidationScan = new TsFileValidationScan(null);
      tsFileValidationScan.scanTsFile(files.get(3));
      assertEquals(0, tsFileValidationScan.getBadFileNum());

      // normal
      tsFileValidationScan = new TsFileValidationScan(null);
      tsFileValidationScan.scanTsFile(files.get(4));
      assertEquals(0, tsFileValidationScan.getBadFileNum());

      // overlap between files
      tsFileValidationScan = new TsFileValidationScan(null);
      tsFileValidationScan.scanTsFile(files.get(3));
      tsFileValidationScan.scanTsFile(files.get(4));
      assertEquals(2, tsFileValidationScan.getBadFileNum());
    } finally {
      files.forEach(File::delete);
    }
  }

  private static List<File> prepareTsFiles() throws IOException {
    List<File> files = new ArrayList<>();
    PlainDeviceID plainDeviceID = new PlainDeviceID("root.sg1.d1");
    // overlap between chunks
    File file = new File(TestConstant.BASE_OUTPUT_PATH, "1.tsfile");
    TsFileResource resource = new TsFileResource(file);
    files.add(file);
    TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(file);
    tsFileIOWriter.startChunkGroup(plainDeviceID);
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
    chunkWriter.write(1, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    chunkWriter.write(1, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    tsFileIOWriter.endChunkGroup();
    tsFileIOWriter.endFile();
    resource.updateStartTime(plainDeviceID, 1);
    resource.updateEndTime(plainDeviceID, 1);
    resource.serialize();

    // overlap between page
    file = new File(TestConstant.BASE_OUTPUT_PATH, "2.tsfile");
    resource = new TsFileResource(file);
    files.add(file);
    tsFileIOWriter = new TsFileIOWriter(file);
    tsFileIOWriter.startChunkGroup(plainDeviceID);
    chunkWriter = new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
    chunkWriter.write(1, 1);
    chunkWriter.sealCurrentPage();
    chunkWriter.write(1, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    tsFileIOWriter.endChunkGroup();
    tsFileIOWriter.endFile();
    resource.updateStartTime(plainDeviceID, 1);
    resource.updateEndTime(plainDeviceID, 1);
    resource.serialize();

    // overlap within page
    file = new File(TestConstant.BASE_OUTPUT_PATH, "3.tsfile");
    resource = new TsFileResource(file);
    files.add(file);
    tsFileIOWriter = new TsFileIOWriter(file);
    tsFileIOWriter.startChunkGroup(plainDeviceID);
    chunkWriter = new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
    chunkWriter.write(1, 1);
    chunkWriter.sealCurrentPage();
    chunkWriter.write(1, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    tsFileIOWriter.endChunkGroup();
    tsFileIOWriter.endFile();
    resource.updateStartTime(plainDeviceID, 1);
    resource.updateEndTime(plainDeviceID, 1);
    resource.serialize();

    // normal
    file = new File(TestConstant.BASE_OUTPUT_PATH, "4.tsfile");
    resource = new TsFileResource(file);
    files.add(file);
    tsFileIOWriter = new TsFileIOWriter(file);
    tsFileIOWriter.startChunkGroup(plainDeviceID);
    chunkWriter = new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
    chunkWriter.write(1, 1);
    chunkWriter.sealCurrentPage();
    chunkWriter.write(2, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    chunkWriter.write(3, 1);
    chunkWriter.sealCurrentPage();
    chunkWriter.write(4, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    tsFileIOWriter.endChunkGroup();
    tsFileIOWriter.endFile();
    resource.updateStartTime(plainDeviceID, 1);
    resource.updateEndTime(plainDeviceID, 4);
    resource.serialize();

    // normal but overlap with 4.tsfile
    file = new File(TestConstant.BASE_OUTPUT_PATH, "5.tsfile");
    resource = new TsFileResource(file);
    files.add(file);
    tsFileIOWriter = new TsFileIOWriter(file);
    tsFileIOWriter.startChunkGroup(plainDeviceID);
    chunkWriter = new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT32));
    chunkWriter.write(3, 1);
    chunkWriter.sealCurrentPage();
    chunkWriter.write(4, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    chunkWriter.write(5, 1);
    chunkWriter.sealCurrentPage();
    chunkWriter.write(6, 1);
    chunkWriter.writeToFileWriter(tsFileIOWriter);
    tsFileIOWriter.endChunkGroup();
    tsFileIOWriter.endFile();
    resource.updateStartTime(plainDeviceID, 3);
    resource.updateEndTime(plainDeviceID, 6);
    resource.serialize();
    return files;
  }
}
