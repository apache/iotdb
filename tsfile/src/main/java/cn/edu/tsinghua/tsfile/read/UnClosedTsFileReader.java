package cn.edu.tsinghua.tsfile.read;

import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadataIndex;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
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
