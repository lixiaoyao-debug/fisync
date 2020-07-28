package com.qixin.fisync.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Dream on 2020/2/14 14:40
 * 用来生成权限配置的文件
 *
 * @author Dream
 */
public class PropertiesProducer {
    private Logger logger = LoggerFactory.getLogger(PropertiesProducer.class);
    private Pattern pattern = Pattern.compile("\\d+$");
    private JsonParser parser = new JsonParser();

    public static void main(String[] args) throws IOException {
        new PropertiesProducer().produce("D:\\code\\java\\analyze\\data_mining\\ToB\\数据交付\\增量(new)" +
                "\\解析\\fisync\\src\\main\\resources\\table_fields.properties");
    }

    public boolean produce(String path) throws IOException {
        StringBuilder result = new StringBuilder();
        File file = new File(path);
        //如果配置存在就备份原来的
        if (file.exists()) {
            FileUtil.move(path, path + ".bak", StandardCopyOption.REPLACE_EXISTING);
        }
        HashMap<String, String> hashMap = new HashMap<>();
        String url = PropertyUtil.getProperty("url");
        String account = PropertyUtil.getProperty("qixin_account").trim();
        String key = PropertyUtil.getProperty("qixin_key").trim();
        String sign = ReportUtil.md5(account + key);
        url = url + "/manager/customer/privilege?account=" + account + "&sign=" + sign + "&method=get";
        String json = HttpUtils.get(url, hashMap);
        JsonObject jsonObject = parser.parse(json).getAsJsonObject();
        JsonObject data = jsonObject.getAsJsonObject("data");
        JsonArray jsonArray = data.getAsJsonArray("privilege");
        StringBuilder stringBuilder = null;
        if (jsonArray == null || jsonArray.size() == 0) {
            logger.error("json has no privilege json is {}", json);
            return false;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject privilege = jsonArray.get(i).getAsJsonObject();
            String dbName = privilege.get("name").getAsString();
            JsonArray tables = privilege.getAsJsonArray("tables");
            Matcher dbMatcher = pattern.matcher(dbName);
            if (dbMatcher.find() && !dbName.endsWith("_0")) {
                continue;
            }
            for (int j = 0; j < tables.size(); j++) {
                stringBuilder = new StringBuilder();
                JsonObject tablesJSONObject = tables.get(j).getAsJsonObject();
                String name = tablesJSONObject.get("name").getAsString();
                Matcher matcher = pattern.matcher(name);
                if (matcher.find() && !name.endsWith("_0")) {
                    continue;
                }
                name = name.replace("_0", "");
                JsonArray fields = tablesJSONObject.getAsJsonArray("fields");
                name = dbName.replace("_0", "") + "," + name;
                stringBuilder.append(name);
                stringBuilder.append(":");
                for (Object o : fields) {
                    stringBuilder.append(o.toString()).append(",");
                }
                result.append(stringBuilder.substring(0, stringBuilder.length() - 1)).append("\n");
            }
            FileUtil.write(path, result.toString());
        }
        return true;
    }

}
