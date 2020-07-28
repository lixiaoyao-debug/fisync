package com.qixin.fisync.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Dream on 2020/4/7 14:16
 *
 * @author Dream
 */
public class StopMergeUtil {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public void stopMerge() {
        try {
            Process process = Runtime.getRuntime().
                    exec("ps -ef |grep binlog_merge |grep java |grep -v grep |awk  '{print $2}'|xargs kill -9");
            int i = process.exitValue();
            if (i == 0) {
                logger.info("success end merge");
            }
        } catch (IOException e) {
            logger.error("stop merge error e", e);
        }
    }
}
