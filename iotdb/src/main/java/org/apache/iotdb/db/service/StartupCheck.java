package org.apache.iotdb.db.service;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StartupException;

public interface StartupCheck {
    /**
     * Run some tests to check whether system is safe to be started
     */
    void execute() throws StartupException;
}
