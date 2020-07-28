package com.qixin.fisync.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 配置加载类
 *
 * @author yangyao
 */
public class PropertyUtil {

    private static final Logger logger=LoggerFactory.getLogger(PropertyUtil.class);
    private static Properties props;


    /**
     * 加载配置文件
     */
    public static void loadProps(String path) {
        //日志信息打印
        logger.info("start load properties .......");
        props = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(path);
            int max = 0x3200000;
            if (in.available() < max) {
                props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
            } else {
                logger.error("file is too large");
            }
        } catch (FileNotFoundException e) {
            logger.error("conf.properties can not found");
        } catch (IOException e) {
            logger.error("IOException" + e.getMessage());
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
                logger.error("conf.properties close error");
            }
        }
        logger.info("load properties file success...........");
        logger.info("properties ：" + props);
    }

    /**
     * 获取配置文件参数
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        if (null == props) {
            loadProps(System.getProperty("propertiesPath"));
        }
        return props.getProperty(key);
    }

    /**
     * 获取配置文件参数，没有则返回默认值
     *
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getProperty(String key, String defaultValue) {
        if (null == props) {
            loadProps(System.getProperty("propertiesPath"));
        }
        return props.getProperty(key, defaultValue);
    }

}
