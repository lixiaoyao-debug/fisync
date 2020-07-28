package com.qixin.fisync.utils;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dream on 2020/2/20 16:15
 * -- 告警级别字符串 = {告警级别数字，描述}
 * ["warn_01"]  = {1, "基础告警"},
 * ["warn_02"]  = {2, "严重告警"},
 * ["error_01"] = {3, "基础错误"},
 * ["error_02"] = {4, "严重错误"},
 *
 * @author Dream
 */
public class Constants {
    public static final String BASIC_WARN = "warn_01";

    public static ConcurrentHashMap<String, Long> errorReport = new ConcurrentHashMap<>();

    public static final String SERIOUS_WARN = "warn_02";

    public static final String BASIC_ERROR = "error_01";

    public static final String SERIOUS_ERROR = "error_02";

    public static Boolean REPORT_START = Boolean.valueOf(PropertyUtil.getProperty("start_report"));


    public static HashMap<String, Integer> map = new HashMap<>();
}
