package cn.edu.thu.tsfiledb.service;

import cn.edu.thu.tsfiledb.exception.StartupException;

public interface StartupCheck {
    /**
     * Run some tests to check whether system is safe to be started
     */
    void execute() throws StartupException;
}
