package org.apache.iotdb.db.engine.compaction.writer;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CrossSpaceCompactionWriter implements ICompactionWriter{
    // target file path -> file writer
    private Map<String, RestorableTsFileIOWriter> writerMap;
    // target file path -> device -> endTime
    private Map<String, Map<String, Long>> timeRangeMap;
    // old source tsfile
    private List<TsFileResource> seqTsFileResources;

    private int seqFileIndex;

    private String deviceId;
    private IChunkWriter chunkWriter;
    private boolean isAlign;

    public CrossSpaceCompactionWriter(List<File> targetFileList, List<TsFileResource> seqFileResources) throws IOException {
        for(File f:targetFileList){
            writerMap.put(f.getPath(),new RestorableTsFileIOWriter(f));
        }
        seqFileIndex=0;
    }

    @Override
    public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
        this.deviceId=deviceId;
        this.isAlign=isAlign;
        for(TsFileResource resource:seqTsFileResources){
            if(resource.isDeviceIdExist(deviceId)){
                writerMap.get(resource.getTsFilePath()+IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX).startChunkGroup(deviceId);
            }
        }
    }

    @Override
    public void endChunkGroup() throws IOException {
        for(TsFileResource resource:seqTsFileResources){
            if(resource.isDeviceIdExist(deviceId)){
                writerMap.get(resource.getTsFilePath()+IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX).startChunkGroup(deviceId);
            }
        }
        deviceId=null;
    }

    @Override
    public void startMeasurement(List<IMeasurementSchema> measurementSchemaList) {
        if(isAlign){
            chunkWriter = new AlignedChunkWriterImpl(measurementSchemaList);
        }else{
            chunkWriter= new ChunkWriterImpl(measurementSchemaList.get(0),true);
        }
    }

    @Override
    public void endMeasurement() throws IOException {
        writeRateLimit(chunkWriter.estimateMaxSeriesMemSize());
        chunkWriter.writeToFileWriter(fileWriter);
        chunkWriter=null;

    }

    @Override
    public void write(long timestamp, Object value) throws IOException {
        while(timestamp > seqTsFileResources.get(seqFileIndex).getEndTime(deviceId)){
            if(chunkWriter.g)
        }
        if(!isAlign){
            ChunkWriterImpl chunkWriter=(ChunkWriterImpl)this.chunkWriter;
            switch (chunkWriter.getDataType()) {
                case TEXT:
                    chunkWriter.write(timestamp, (Binary) value);
                    break;
                case DOUBLE:
                    chunkWriter.write(timestamp, (Double) value);
                    break;
                case BOOLEAN:
                    chunkWriter.write(timestamp, (Boolean) value);
                    break;
                case INT64:
                    chunkWriter.write(timestamp, (Long) value);
                    break;
                case INT32:
                    chunkWriter.write(timestamp, (Integer) value);
                    break;
                case FLOAT:
                    chunkWriter.write(timestamp, (Float) value);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
            }
        }else{
            value=((TsPrimitiveType[]) value)[0].getValue();
            TSDataType tsDataType=((TsPrimitiveType[]) value)[0].getDataType();
            boolean isNull= value==null;
            AlignedChunkWriterImpl chunkWriter=(AlignedChunkWriterImpl)this.chunkWriter;
            switch (tsDataType) {
                case TEXT:
                    chunkWriter.write(timestamp, (Binary) value,isNull);
                    break;
                case DOUBLE:
                    chunkWriter.write(timestamp, (Double) value,isNull);
                    break;
                case BOOLEAN:
                    chunkWriter.write(timestamp, (Boolean) value,isNull);
                    break;
                case INT64:
                    chunkWriter.write(timestamp, (Long) value,isNull);
                    break;
                case INT32:
                    chunkWriter.write(timestamp, (Integer) value,isNull);
                    break;
                case FLOAT:
                    chunkWriter.write(timestamp, (Float) value,isNull);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown data type " + tsDataType);
            }
        }
        if(chunkWriter.estimateMaxSeriesMemSize()>2*1024*1024){ //Todo:
            endMeasurement();
        }
    }

    @Override
    public void write(long[] timestamps, Object values) {

    }

    @Override
    public void endFile() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    private static void writeRateLimit(long bytesLength) {
        MergeManager.mergeRateLimiterAcquire(
                MergeManager.getINSTANCE().getMergeWriteRateLimiter(), bytesLength);
    }
}
