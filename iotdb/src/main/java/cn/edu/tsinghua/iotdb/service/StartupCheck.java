package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.exception.StartupException;

public interface StartupCheck {
    /**
     * Run some tests to check whether system is safe to be started
     */
    void execute() throws StartupException;
}
