package cn.edu.tsinghua.iotdb.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoUpload {

    private static Logger logger = LoggerFactory.getLogger(NoUpload.class);

    public static void main(String[] args) {
        logger.error("un defined CrossRelation, left time pair is [{},{}], " +
                        "right time pair is {}, {}",
                1, 2, 3, 4);
    }
}
