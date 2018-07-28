package cn.edu.tsinghua.iotdb.exception.builder;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.Properties;

public class ExceptionBuilder {
    private Properties properties = new Properties();

    public static final int UNKNOWN_ERROR = 20000;
    public static final int NO_PARAMETERS_EXISTS=20001;
    public static final int INVALID﻿_PARAMETER_NO=20002;
    public static final int CONN_HOST_ERROR=20003;
    public static final int AUTH_PLUGIN_ERR=20061;
    public static final int INSECURE_API_ERR=20062;
    public static final int OUT_OF_MEMORY=20064;
    public static final int NO_PREPARE_STMT=20130;
    public static final int CON_FAIL_ERR=20220;


    private static final Logger LOGGER = LoggerFactory.getLogger(TsfileDBDescriptor.class);
    public static final String CONFIG_NAME= "error_info_";
    public static final String FILE_SUFFIX=".properties";
    public static final String DEFAULT_FILEPATH="error_info_en.properties";

    private static final ExceptionBuilder INSTANCE = new ExceptionBuilder();
    public static final ExceptionBuilder getInstance() {
        return ExceptionBuilder.INSTANCE;
    }

    public void loadInfo(String filePath){
        InputStream in = null;
        try {
            in = new BufferedInputStream (new FileInputStream(filePath));
            properties.load(new InputStreamReader(in,"utf-8"));
            in.close();
        } catch (IOException e) {
            LOGGER.error("Read file error. File does not exist or file is broken. File path: {}.Because: {}.",filePath,e.getMessage());
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOGGER.error("Fail to close file: {}. Because: {}.",filePath,e.getMessage());
                }
            }
        }
    }

    public void loadInfo(){
        String language = TsfileDBDescriptor.getInstance().getConfig().languageVersion.toLowerCase();

        String url = System.getProperty(TsFileDBConstant.IOTDB_CONF, null);
        if (url == null) {
            url = System.getProperty(TsFileDBConstant.IOTDB_HOME, null);
            if (url != null) {
                    url = url + File.separatorChar + "conf" + File.separatorChar + ExceptionBuilder.CONFIG_NAME+language+FILE_SUFFIX;
            } else {
                LOGGER.warn("Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration", TsfileDBConfig.CONFIG_NAME);
                return;
            }
        } else{
            url += (File.separatorChar + ExceptionBuilder.CONFIG_NAME+language+FILE_SUFFIX);
        }

        File file = new File(url);
        if(!file.exists()){
            url.replace(CONFIG_NAME+language+FILE_SUFFIX, DEFAULT_FILEPATH);
        }

        loadInfo(url);
    }
    public String searchInfo(int errCode){
        return properties.getProperty(String.valueOf(errCode));
    }
}
