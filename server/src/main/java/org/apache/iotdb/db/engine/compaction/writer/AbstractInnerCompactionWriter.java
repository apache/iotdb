package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class AbstractInnerCompactionWriter implements ICompactionWriter{
    protected TsFileIOWriter fileWriter;

    protected boolean isEmptyFile;

    protected boolean isAlign;

    protected String deviceId;

    public AbstractInnerCompactionWriter(TsFileResource targetFileResource) throws IOException {
        this.fileWriter = new TsFileIOWriter(targetFileResource.getTsFile());
        isEmptyFile = true;
    }

    @Override
    public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
        fileWriter.startChunkGroup(deviceId);
        this.isAlign = isAlign;
        this.deviceId = deviceId;
    }

    @Override
    public void endChunkGroup() throws IOException {
        fileWriter.endChunkGroup();
    }

    @Override
    public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
        measurementPointCountArray[subTaskId] = 0;
        if (isAlign) {
            chunkWriters[subTaskId] = new AlignedChunkWriterImpl(measurementSchemaList);
        } else {
            chunkWriters[subTaskId] = new ChunkWriterImpl(measurementSchemaList.get(0), true);
        }
    }

    @Override
    public void endMeasurement(int subTaskId) throws IOException {
        CompactionWriterUtils.flushChunkToFileWriter(fileWriter, chunkWriters[subTaskId]);
    }

    @Override
    public abstract void write(long timestamp, Object value, int subTaskId) throws IOException ;

    @Override
    public abstract void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize) throws IOException ;

    @Override
    public void endFile() throws IOException {
        fileWriter.endFile();
        if (isEmptyFile) {
            fileWriter.getFile().delete();
        }
    }

    @Override
    public void close() throws Exception {
        if (fileWriter != null && fileWriter.canWrite()) {
            fileWriter.close();
        }
        fileWriter = null;
    }

    @Override
    public List<TsFileIOWriter> getFileIOWriter() {
        return Collections.singletonList(fileWriter);
    }
}
