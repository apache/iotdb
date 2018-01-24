package cn.edu.tsinghua.iotdb.writelog.recover;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeRecoverPerformer implements RecoverPerformer {

    private static final Logger logger = LoggerFactory.getLogger(FileNodeRecoverPerformer.class);

    /**
     * If the storage group is set at "root.a.b", then the identifier for a bufferwrite processor will be "root.a.b-bufferwrite",
     * and the identifier for an overflow processor will be "root.a.b-overflow".
     */
    private String identifier;

    public FileNodeRecoverPerformer(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public void recover() throws RecoverException {
        try {
            FileNodeManager.getInstance().recoverFileNode(getFileNodeName());
        } catch (FileNodeProcessorException | FileNodeManagerException e) {
            logger.error("Cannot recover filenode {}", identifier);
            throw new RecoverException(e);
        }
    }

    public String getFileNodeName() {
        return identifier.split("-")[0];
    }
}
