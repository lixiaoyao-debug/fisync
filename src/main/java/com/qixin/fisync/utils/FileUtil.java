package com.qixin.fisync.utils;

import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Created by Dream on 2020/2/11 16:13
 *
 * @author Dream
 */
public class FileUtil {
    //字段分隔符
    private static String FIELD_REGX = PropertyUtil.getProperty("FIELD_REGX");

    //行分割符
    private static String LINE_REGX = PropertyUtil.getProperty("LINE_REGX");

    public static final int MIN_INDEX = 0;
    public static final int MAX_INDEX = 15;
    public static final String TABLE_PREFIX = "t";
    private static final String FILE_SUFFIX = ".log";
    private static String HIVE_NULL_CHARACTER = "";
    private static final Pattern PATTERN = Pattern.compile("^[-+]?[\\d]*$");
    private static Map<String, List<String>> tableFieldsMap = new HashMap<>(3);
    private static Logger logger = LoggerFactory.getLogger(FileUtil.class.getName());
    private static StringBuilder outputStringBuilder = new StringBuilder();
    private static JsonParser parser = new JsonParser();


    /**
     * 1.按行读取log文件内容
     * 2.替换特殊字符
     *
     * @param file : 输入文件
     */
    public static boolean readFile(File file, String tableName, String originTable) {
        //如果文件内容为空，直接跳出
        if (file.length() == 0) {
            return true;
        }
        BufferedReader reader = null;
        int lineNum = 0;
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
            reader = new BufferedReader(inputStreamReader);
            String tempString;
            //截取解析文件名5-15，file-2020-07-09-0000.log，即截取到的是日期2020-07-09
            String name = file.getName().substring(5, 15);

            //定义一个统计operate个数的属性
            int operNum = 0;

            //按行读取文件，没有就跳出
            while ((tempString = reader.readLine()) != null) {
                //记录读取的行数
                lineNum = lineNum + 1;
                //替换数据中的固定分隔符,将固定分隔符替换为空格
                String curLine = tempString.replaceAll("[\n\r\\001]", " ");
                //解析.log文件的每一行,并返回这行数据中operate的个数
                int num = parseJson(curLine, tableName, originTable);
                operNum += num;
            }


            //打印日志，计算operate的个数
            logger.info("Source:" + file.getAbsolutePath() + "\tOperate:" + operNum);


            //表名+日期；enterprise,2020-07-24
            String finalName = tableName + "," + name;
            //能否从map集合中拿到
            Integer integer = Constants.map.get(finalName);
            //拿不到
            if (integer == null) {
                //则按照key,value格式放入，行号作为value
                Constants.map.put(finalName, lineNum);
            } else {
                //拿到，则将行号相加
                Constants.map.put(finalName, integer + lineNum);
            }
            return true;
        } catch (Exception e) {
            logger.error(file.toString(), e);
            return false;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
//                    Long aLong = errorReport.get(e.getMessage());
//                    Long now = System.currentTimeMillis();
//                    if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
//                        errorReport.put(e.getMessage(), now);
//                        ReportUtil.reportLog("error", "FileUtil-85" + e.getMessage());
//                    }
                    logger.error(file.toString(), e);
                }

            }
        }
    }

    /**
     * 1. Json字符串解析为Json数组
     * 2. Json数组解析为Json对象，每个Json对象是两层嵌套结构，外层确定内层主键"PK"值
     * 3. 用两层LinkedHashMap保存,可以保证输出结果的有序性
     * 外层key： 每条log主键
     * value：json对象完整内容
     * 内层key-value: json对象的key-value
     *
     * @param jsonContent log文件每一行的Json字符串
     */
    //解析下载路径下的数据，解析后的结果也就是output路径下的数据
    private static int parseJson(String jsonContent, String tableName, String originTable) {
        //定义一个统计Operate个数的属性
        int operNum = 0;

        //通过tableName获得表属性
        List<String> tableFields = tableFieldsMap.get(tableName);
        if (tableFields == null || tableFields.isEmpty()) {
            return 0;
        }
        //获取json数组对象，jsonContent log文件每一行的Json字符串
        JsonArray objects = parser.parse(jsonContent).getAsJsonArray();
        try {
            //遍历这个json数组
            for (Object object : objects) {
                //获得json对象
                JsonObject jsonObject = parser.parse(String.valueOf(object)).getAsJsonObject();
                StringBuilder outputLineStringBuilder = new StringBuilder();
                if (jsonObject.has("fields") && jsonObject.get("fields") != null) {
                    //获得fileds这个json的value，即某张表的所有fields
                    JsonObject fieldsJsonObject = jsonObject.get("fields").getAsJsonObject();
                    for (String tableField : tableFields) {
                        JsonElement value = fieldsJsonObject.get(tableField);
                        if (value == null) {
                            outputLineStringBuilder.append(HIVE_NULL_CHARACTER).append(FIELD_REGX);
                        } else {
                            outputLineStringBuilder.append(value.getAsString().replaceAll("[\r\n\\001]", " ")).append(FIELD_REGX);
                        }
                    }
                    //在最后追加三个字段的数据
                    //字段origin_table
                    outputLineStringBuilder.append(originTable).append(FIELD_REGX);

                    //对获得的oprate内容进行规定
                    //字段operate
                    JsonElement oper = jsonObject.get("operate");
                    operNum += 1;
                    //result=0直接默认更新，因为这种update的数据最多
                    String result = "666";
                    if("UPDATE".equals(oper.getAsString())){
                        result="0";
                    }else if ("INSERT".equals(oper.getAsString())) {
                        result = "1";
                    } else if ("DELETE".equals(oper.getAsString())) {
                        result = "2";
                    }
                    outputLineStringBuilder.append(result).append(FIELD_REGX);

                    //字段order_id
                    outputLineStringBuilder.append(System.nanoTime()).append(LINE_REGX);
                } else {
                    continue;
                }
                //最后把解析好的数据追加到全局变量中
                //outputLineStringBuilder中即使对下载路径下某行文件的解析数据
                outputStringBuilder.append(outputLineStringBuilder);
            }
        } catch (Exception e) {
            logger.error("parseJson error ", e);
//            Long aLong = errorReport.get(e.getMessage());
//            Long now = System.currentTimeMillis();
//            if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
//                errorReport.put(e.getMessage(), now);
//                ReportUtil.reportLog("error", "FileUtil-139" + e.getMessage());
//            }
        }
        return operNum;
    }

    private static <T> void setColumnValue(String columnValue, T t, String columnName) throws NoSuchFieldException, IllegalAccessException {
        Field field = t.getClass().getDeclaredField(columnName);
        if (field != null) {
            field.setAccessible(true);
            field.set(t, columnValue);
        }
    }

    /**
     * 根据指定属性获取指定对象的属性值
     * 外层Json的属性"pk" 指定内层的主键属性名 e.g pk:id
     * 遍历对象的get方法，找到对应的属性值
     *
     * @param ob   对象类名
     * @param name 属性名
     */
    private static Object getGetMethod(Object ob, String name) throws Exception {
        Method[] m = ob.getClass().getMethods();
        for (Method method : m) {
            if (("get" + name).toLowerCase().equals(method.getName().toLowerCase())) {
                return method.invoke(ob);
            }
        }
        return null;
    }

    /**
     * 组合数组结果并输出到文件
     *
     * @param outputPath 输出文件的绝对路径
     */
    public static boolean writeFile(String outputPath) {
        String outputString = outputStringBuilder.toString();
        //输出路径不为空
        if ("".equals(outputString)) {
            return true;
        }
        BufferedWriter writer = null;
        File file = new File(outputPath);
        try {
            FileOutputStream fos;
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    logger.error("Cannot create Dir" + outputPath);
                }
            }
            fos = new FileOutputStream(file);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            writer = new BufferedWriter(outputStreamWriter);
            writer.write(outputString);
            outputStringBuilder.delete(0, outputStringBuilder.length());
//            writer.write(concatOutputString());
//            map.clear();
            return true;
        } catch (IOException e) {
            logger.error("writeFile error", e);
//            Long aLong = errorReport.get(e.getMessage());
//            Long now = System.currentTimeMillis();
//            if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
//                errorReport.put(e.getMessage(), now);
//                ReportUtil.reportLog(Constants.BASIC_ERROR, "FileUtil-200" + e.getMessage());
//            }
            return false;
        } finally {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    logger.error(file.toString(), e);
                }
            }
        }
    }


    /**
     * 1.组合输出结果:
     *      外层输出"operate"，内层按属性名称依次输出
     * 2. 字段分隔符： '/0001', 行分隔符： '/n'
     */
//    private static String concatOutputString(){
//        StringBuilder sb = new StringBuilder();
//        if(!map.isEmpty()){
//            for(Map.Entry<String, Object> jsonEntry : map.entrySet()){
//                String single = reflectGetAttribute(jsonEntry.getValue());
//                sb.append(single.substring(0, single.length()-getFieldRegx().length())).append(getLineRegx());
//            }
//        }
//        return sb.toString();
//    }

    /**
     * 组装行数据：
     * 通过反射获取自定义Bean的属性名
     * 组合属性名的get方法，获取属性值
     *
     * @param model 对象名
     */
    private static String reflectGetAttribute(Object model) {
        StringBuilder sb = new StringBuilder();
        Field[] field = model.getClass().getDeclaredFields();
        for (Field aField : field) {
            String name = aField.getName();
            name = name.substring(0, 1).toUpperCase() + name.substring(1);
            String type = aField.getGenericType().toString();
            if ("class java.lang.String".equals(type)) {
                Method m;
                try {
                    m = model.getClass().getMethod("get" + name);
                    String value = (String) m.invoke(model);
                    if (value != null) {
                        String tmpString = value.replaceAll("[\n\r\\001]", " ");
                        sb.append(tmpString).append(getFieldRegx());
                    }
                } catch (Exception e) {
                    logger.error("reflectGetAttribute error", e);
                }
            }
        }
        return sb.toString();
    }

    /**
     * db 和 table 分库表序号 转 Hex
     */
    public static boolean isInteger(String str) {
        return PATTERN.matcher(str).matches();
    }

    public static boolean isIndex(String lastIndex) {
        if (isInteger(lastIndex)) {
            int index = Integer.parseInt(lastIndex);
            return (index >= MIN_INDEX) && (index <= MAX_INDEX);
        }
        return false;
    }

    private static String getFieldRegx() {
        return FIELD_REGX;
    }

    public static void setFieldRegx(String fieldRegx) {
        FIELD_REGX = fieldRegx;
    }

    private static String getLineRegx() {
        return LINE_REGX;
    }

    public static void setLineRegx(String lineRegx) {
        LINE_REGX = lineRegx;
    }

    public static String getHiveNullCharacter() {
        return HIVE_NULL_CHARACTER;
    }

    public static void setHiveNullCharacter(String hiveNullCharacter) {
        HIVE_NULL_CHARACTER = hiveNullCharacter;
    }

    public static Map<String, List<String>> getTableFieldsMap() {
        return tableFieldsMap;
    }

    public static void setTableFieldsMap(Map<String, List<String>> tableFieldsMap) {
        FileUtil.tableFieldsMap = tableFieldsMap;
    }

    public static void createEmptyFile(String logPath, File file) {
        File filePath = new File(logPath);
        if (!filePath.exists()) {
            if (!filePath.mkdirs()) {
                logger.error("Cannot create Dir" + filePath.getName());
            }
        }
        File emptyFile = new File(filePath, file.getName());
        if (!emptyFile.exists()) {
            try {
                if (!emptyFile.createNewFile()) {
                    logger.error("Cannot create file" + emptyFile.getName());
                }
            } catch (IOException e) {
                logger.error("createEmptyFile", e);
            }
        }
    }

    public static String getConfigValue(Properties prop, String configName, String defaultValue) {
//        return prop.getProperty(configName).isEmpty() ? defaultValue : prop.getProperty(configName);
        return prop.getProperty(configName, defaultValue);
    }

    //通过tableFieldsConfigFile的路径设置表属性，tableFieldsConfigFile即使table_fields.config中的库表配置信息，即这次需要交付的库表配置信息
    public static void setTableFields(File tableFieldsConfigFile) {
        try {
            FileInputStream fileInputStream = new FileInputStream(tableFieldsConfigFile);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
            //输入流读数据
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String tempString;
            //每次读一行，没有跳出循环，每一行代表一个表的属性配置
            while ((tempString = bufferedReader.readLine()) != null) {
                //对读到的这行数据先进行两头去重
                String line = tempString.trim();
                if ("".equals(line)) {
                    continue;
                }
                //按照：进行分割
                String[] temp = line.split(":");
                //正常分割后的数组长度为2
                if (temp.length == 2) {
                    //创建一个装表属性的集合
                    List<String> tableFieldsList = new ArrayList<>();
                    //获得表名，且是去掉t_前缀的表名
                    String tableName = temp[0].trim().split(",")[1].replaceFirst("t_", "");
                    String tableFields = temp[1].trim();
                    //所有表属性的数组
                    String[] tableFieldsArray = tableFields.split(",");
                    for (String tableField : tableFieldsArray) {
                        //遍历该数组，对每一个数组进行首尾去空，并把它加入tableFieldsList
                        tableFieldsList.add(tableField.trim());
                    }
                    //最后将（表名，表属性集合）放入表属性的Map中tableFieldsMap，且表名是不带t_前缀的，即stock_struct，rgincome
                    tableFieldsMap.put(tableName, tableFieldsList);
                }
            }
        } catch (Exception e) {
            logger.error("read table fields config file error : ", e);
//            Long aLong = errorReport.get(e.getMessage());
//            Long now = System.currentTimeMillis();
//            if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
//                errorReport.put(e.getMessage(), now);
//                ReportUtil.reportLog(Constants.BASIC_ERROR, "FileUtil-363" + e.getMessage());
//            }
        }
    }

    /**
     * 获取指定路径下的所有没有被写的文件
     *
     * @param path 指定路径
     * @return 所有文件
     */
    //获取下载路径下所有没有正在被写的文件
    public static List<File> getAllFiles(String path) {
        return getAllFiles(new File(path));
    }

    //方法的重载
    private static List<File> getAllFiles(File path) {
        return getPathFiles(path);
    }

    /**
     * 获取文件下的所有文件
     *
     * @param file 文件夹路径
     * @return 文件夹
     */
    //获取下载路径下的所有文件
    private static List<File> getPathFiles(File file) {
        List<File> files = new ArrayList<>();
        //判断路径是不是文件夹
        if (file.isDirectory()) {
            //是文件夹，则获得该文件夹下的所有文件
            File[] allFile = file.listFiles();

            if (allFile != null) {
                //遍历这些文件
                for (File listFile : allFile) {
                    //内外再遍历，将所有文件放入集合中
                    files.addAll(getPathFiles(listFile));
                }
            }
        } else {
            //如果不是文件夹，则是文件
            //检查这个文件是否正在被写入，如果没有，则为true
            if (checkFileWriting(file)) {
                //将文件加入集合
                files.add(file);
            }
        }

        //过滤出files集合中所有以.log结尾的文件
        return files.stream().filter(file1 -> file1.getName().endsWith(FILE_SUFFIX)).sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
    }

    /**
     * 判断文件是否正在被写
     * 给文件加读写锁，能拿到锁说明此文件已经写完全，直接释放锁
     * 如果不能拿到锁说明次文件正被其他线程写，等待10ms再次获取锁
     * 重复10次，还是拿不到锁则跳过此文件
     *
     * @param file
     * @return
     */
    private static boolean checkFileWriting(File file) {
        RandomAccessFile raf = null;
        FileChannel channel = null;
        FileLock lock;
        int count = 0;
        boolean flag = true;
        try {
            raf = new RandomAccessFile(file, "rw");
            channel = raf.getChannel();
            while (true) {
                try {
                    if (count > 10) {
                        flag = false;
                        break;
                    }
                    lock = channel.tryLock();
                    if (null == lock) {
                        count++;
                        TimeUnit.MILLISECONDS.sleep(10);
                    } else {
                        lock.release();
                        flag = true;
                        break;
                    }
                } catch (Exception e) {
                    count = Integer.MAX_VALUE;
                    logger.error(file.toString(), e);
                }
            }
        } catch (Exception e) {
            logger.error(file.toString(), e);
//            Long aLong = errorReport.get(e.getMessage());
//            Long now = System.currentTimeMillis();
//            if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
//                errorReport.put(e.getMessage(), now);
//                ReportUtil.reportLog(Constants.BASIC_WARN, "FileUtil-447" + e.getMessage());
//            }
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    logger.error(file + "\t" + e.getMessage());
                }
            }
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
//                    Long aLong = errorReport.get(e.getMessage());
//                    Long now = System.currentTimeMillis();
//                    if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
//                        errorReport.put(e.getMessage(), now);
//                        ReportUtil.reportLog(Constants.BASIC_WARN, "FileUtil-465" + e.getMessage());
//                    }
                    logger.error(file.toString(), e);
                }
            }
        }
        return flag;
    }

    /**
     * 将已经解析的文件移动到待删目录
     *
     * @param source  源文件路径
     * @param target  新文件路径
     * @param options 移动操作
     * @throws IOException
     */
    //直接移动到待删除目录下
    public static void move(String source, String target, CopyOption... options) throws IOException {
        File file = new File(target);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        Files.move(Paths.get(source), Paths.get(target), options);
    }

    public static void write(String path, String body) {
        File file = new File(path);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try {
            FileWriter writer = new FileWriter(file);
            writer.write(body);
            writer.flush();
        } catch (IOException e) {
//            Long aLong = errorReport.get(e.getMessage());
//            Long now = System.currentTimeMillis();
//            if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
//                errorReport.put(e.getMessage(), now);
//                ReportUtil.reportLog(Constants.BASIC_WARN, "FileUtil-504" + e.getMessage());
//            }
            logger.error("write file error", e);
        }
    }
}
