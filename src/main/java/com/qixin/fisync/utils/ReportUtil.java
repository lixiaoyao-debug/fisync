package com.qixin.fisync.utils;

import com.google.gson.JsonObject;
import org.apache.commons.codec.digest.DigestUtils;


import java.util.HashMap;

/**
 *
 */
public class ReportUtil {
    public static String reportLog(String level, String msg) {
        if (!Constants.REPORT_START) {
            return "success";
        }
        String url = PropertyUtil.getProperty("url");
        url = url + "/logmonitor/logmessage";
        JsonObject object = new JsonObject();
        object.addProperty("timestamp", String.valueOf(System.currentTimeMillis() / 1000L));
        object.addProperty("subsys", "QXB_BIGDATA_INCREMENT_MERGE");
        object.addProperty("module", "bigdata-fisync");
        object.addProperty("level", level);
        object.addProperty("msg", msg);
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("X-Real-IP", PropertyUtil.getProperty("real_ip"));
        hashMap.put("X-Is-Host", PropertyUtil.getProperty("real_ip"));
        return HttpUtils.post(url, object.getAsString(), hashMap);
    }

    public static String heartbeat(String msg) {
        if (!Constants.REPORT_START) {
            return "success";
        }
        String url = PropertyUtil.getProperty("url");
        url = url + "/manager/client/heartbeat";
        JsonObject object = new JsonObject();
        object.addProperty("real_ip", "10.2.18.100");
        object.addProperty("interval", "5");
        object.addProperty("client", "bigdata-fisync");
        object.addProperty("msg", "hearting");
        object.addProperty("msg_type", "heart");
        object.addProperty("account", "test_suzhou");
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("X-Real-IP", PropertyUtil.getProperty("real_ip"));
        hashMap.put("X-Is-Host", PropertyUtil.getProperty("real_ip"));
        return HttpUtils.post(url, object.toString(), hashMap);
    }


    public static String md5(String msg) {
        return DigestUtils.md5Hex(msg);
    }

    public static void main(String[] args) {
        System.out.println(md5("test_suzhou" + "4a8754111f995a8c241d50023f58dd61"));
    }
}
