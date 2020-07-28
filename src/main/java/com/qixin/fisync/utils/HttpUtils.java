package com.qixin.fisync.utils;


import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.io.*;
import java.util.HashMap;

/**
 * Created by Dream on 2020/2/13 14:24
 *
 * @author Dream
 */
public class HttpUtils {
    static CloseableHttpClient httpClient;
    static Logger logger = LoggerFactory.getLogger(HttpUtils.class);
    private static HashMap<Throwable, Long> errorReport = new HashMap<>();

    static {
        httpClient = HttpClients.createDefault();
    }

    public static void main(String[] args) throws IOException {
        String url = "http://101.37.78.221:18088/ksrcb/api/hbase/scanTable";
        String entity = "{\"tableName\":\"qkgs:cmp_corp_map\"," +
                "\"eid\":\"e0141427-e555-44bb-94c3-706765159118\"}";

        System.out.println(post(url, entity, new HashMap<>()));
    }

    public static String post(String url, String jsonEntity, HashMap<String, String> header) {
        if (url == null) {
            return "";
        }
        HttpPost post = new HttpPost(url);
        header.forEach(post::addHeader);
        post.setEntity(new StringEntity(jsonEntity, ContentType.APPLICATION_JSON));
        CloseableHttpResponse response = null;
        StringBuilder sb = new StringBuilder();
        try {
            response = httpClient.execute(post);
            InputStream stream = response.getEntity().getContent();
            InputStreamReader reader = new InputStreamReader(stream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String s = "";
            while ((s = bufferedReader.readLine()) != null) {
                sb.append(s);
            }
        } catch (IOException e) {
            logger.error("http error", e);
        }
        return sb.toString();
    }


    public static String get(String url, HashMap<String, String> header) {
        HttpGet get = new HttpGet(url);
        header.forEach(get::addHeader);
        CloseableHttpResponse response = null;
        StringBuilder sb = new StringBuilder();
        try {
            response = httpClient.execute(get);
            InputStream stream = response.getEntity().getContent();
            InputStreamReader reader = new InputStreamReader(stream);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String s = "";
            while ((s = bufferedReader.readLine()) != null) {
                sb.append(s);
            }
        } catch (IOException e) {
            logger.error("http error", e);
        }
        return sb.toString();
    }


}
