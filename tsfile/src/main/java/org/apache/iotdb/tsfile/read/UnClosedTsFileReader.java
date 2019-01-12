package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

/**
 * A class for reading unclosed tsfile.
 */
public class UnClosedTsFileReader extends TsFileSequenceReader {

    public UnClosedTsFileReader(String file) throws IOException {
        super(file, false);
    }

    /**
     * unclosed file has no tail magic data
     */
    @Override
    public String readTailMagic() throws IOException {
        throw new NotImplementedException();
    }

    /**
     * unclosed file has no file metadata
     */
    @Override
    public TsFileMetaData readFileMetadata() throws IOException {
        throw new NotImplementedException();
    }

    /**
     * unclosed file has no  metadata
     */
    @Override
    public TsDeviceMetadata readTsDeviceMetaData(TsDeviceMetadataIndex index) throws IOException {
        throw new NotImplementedException();
    }
}
