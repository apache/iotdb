package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.exception.RecoverException;

public interface RecoverPerformer {
    /**
     * Start the recovery process of the module to which this object belongs.
     * @throws RecoverException
     */
    void recover() throws RecoverException;
}
