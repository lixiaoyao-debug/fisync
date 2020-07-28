package com.qixin.fisync.parse;

import com.qixin.fisync.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.qixin.fisync.utils.FileUtil.*;

/**
 * Created by Dream on 2020/2/11 16:10
 *
 * @author Dream
 */
public class IncreaseParse {
    //表前缀
    private static final String TABLE_PREFIX = "t";
    //文件后缀
    public static final String FILE_SUFFIX = ".log";
    private static Logger logger = LoggerFactory.getLogger(IncreaseParse.class);
    private String outputPath;
    //正则表达式匹配
    private static Pattern pattern = Pattern.compile(".*_\\d{1,2}$");

    //有参构造
    public IncreaseParse(String outputPath) {
        this.outputPath = outputPath;
    }

    public void onFileCreate(File file) throws IOException {
        Files.move(Paths.get(file.getPath()), Paths.get("ddd"));
    }

    //解析下载路径下的文件内容
    public void parseJsonFile(File file, String deletePath) {
        //从下载路径下获得表名,other_information
        String pureTableName = getTableName(file);
        //获得file文件的绝对路径
        String absolutePath = file.getAbsolutePath();
        //下载文件范围超过需要范围，只解析交付范围内表的增量文件
        //从tableFieldsMap中，通过表名获取内容，获得内容为Null就不是交付范围，打印错误日志
        if (FileUtil.getTableFieldsMap().get(pureTableName) == null) {
            logger.info(" Unreachable File: " + absolutePath);
            if (file.exists() && file.isFile() && file.delete()) {
                logger.info("Delete file:" + file.getAbsolutePath());
            } else {
                logger.error("Delete file error:" + file.getAbsolutePath());
            }
            return;
        }
        //获取分库分表的16进制,获得分库分表的16进制号
        String originTable = getOriginTable(file);
        if (originTable == null) {
            return;
        }
        //获得输出文件名，即output路径下的文件名称
        String outputFileName = getOutputFileName(file, originTable);

        //获得输出文件路径
        //output/db_enterpriset_enterprise/ff_file-2020-07-24-0002.log
        String outputFileAbsolutePath = getOutputFilePath(file, outputFileName);
        try {
            //按行读取log文件内容，并替换特殊字符
            boolean readResult = FileUtil.readFile(file, pureTableName, originTable);
            //组合数组结果，并输出到文件，这里的outputFileAbsolutePath即是output/db_enterpriset_enterprise/ff_file-2020-07-24-0002.log
            boolean writeResult = FileUtil.writeFile(outputFileAbsolutePath);
            //如果输出路径已经存在，打印日志信息
            if ((new File(outputFileAbsolutePath).exists())) {
                logger.info(" File Create: " + absolutePath + "\t" + "Output Name: " + outputFileName);
            }
            //获得表名，t_last_industry_9
            String tableName = file.getParentFile().getName();
            //获得库名，db_sub_enterprises_9
            String dbName = file.getParentFile().getParentFile().getName();
//            String logDir = logPath + "/" + pureTableName + "/" + dbName + "/" + tableName;
            String sourcePath = file.getPath().replace("\\", "/");
            //拼接删除路径，待删除路径和下载路径后面是一样的
            deletePath = deletePath + "/" + dbName + "/" + tableName + "/" + file.getName();
            logger.info("sourcePath is{}  && targetPath is {}", sourcePath, deletePath);
            if (readResult && writeResult) {
                //将解析路径下的文件移动到待删除路径下
                FileUtil.move(sourcePath, deletePath, StandardCopyOption.REPLACE_EXISTING);
                logger.info("success move file");
            }
        } catch (Exception e) {
            logger.error(" Parse File Error:", e);
        }
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

    //获得分库分表序号的16进制
    private String getOriginTable(File file) {
        //表名数组
        String[] tableArrays = file.getParentFile().getName().split("_");
        //库名数组
        String[] dbArrays = file.getParentFile().getParentFile().getName().split("_");
        if (tableArrays.length == 0 || dbArrays.length == 0) {
            return null;
        }
        //获取表名最后一位
        String tableIndex = tableArrays[tableArrays.length - 1];
        //判断最后一位是否是数字，如果是转为16进制，如果不是赋值为0
        String tableIndexHex = isIndex(tableIndex) ? Integer.toHexString(Integer.parseInt(tableIndex)) : "0";
        //获取库名的最后一位
        String dbIndex = dbArrays[dbArrays.length - 1];
        //判断是否为数字，是转为16进制，不是直接赋值0
        String dbIndexHex = isIndex(dbIndex) ? Integer.toHexString(Integer.parseInt(dbIndex)) : "0";
        //最后返回分表序号+分库序号的十六进制
        return tableIndexHex + dbIndexHex;
    }

    //获得输出文件路径
    private String getOutputFilePath(File file, String outputFileName) {
//        return this.outputPath + "/" + getDateString(file) + "/" + bean + "/"+ outputFileName;
        //获得库名
        String dbName = file.getParentFile().getParentFile().getName();
        StringBuilder sb = new StringBuilder();
        //获得表名
        String tableName = file.getParentFile().getName();
        //匹配库名是否符合正则
        Matcher matcher = pattern.matcher(dbName);
        if (matcher.find() && pattern.matcher(tableName).find()) {
            String[] dbSplits = dbName.split("_");
            String[] tableSplits = tableName.split("_");
            for (int i = 0; i < dbSplits.length; i++) {
                if (i != dbSplits.length - 1) {
                    if (i != dbSplits.length - 2) {
                        sb.append(dbSplits[i]).append("_");
                    } else {
                        //反正就是最后拼接出来的库名没有库号
                        sb.append(dbSplits[i]);
                    }
                }
            }


            for (int i = 0; i < tableSplits.length; i++) {
                if (i != tableSplits.length - 1) {
                    if (i != tableSplits.length - 2) {
                        sb.append(tableSplits[i]).append("_");
                    } else {
                        //最后获取表名且不带序号
                        sb.append(tableSplits[i]);
                    }
                }
            }
        } else if (pattern.matcher(tableName).find()) {
            sb.append(dbName);
            String[] tableSplits = tableName.split("_");
            for (int i = 0; i < tableSplits.length; i++) {
                if (i != tableSplits.length - 1) {
                    if (i != tableSplits.length - 2) {
                        sb.append(tableSplits[i]).append("_");
                    } else {
                        sb.append(tableSplits[i]);
                    }
                }
            }
        } else {
            sb.append(dbName).append(tableName);
        }
        //最后返回，outputPath/dbName+tableName/outputFileName
        // output/db_enterpriset_enterprise/ff_file-2020-07-24-0002.log
        return this.outputPath + "/" + new String(sb) + "/" + outputFileName;
    }


    /**
     * 获得输出路径的库表名，不包括文件
     * @param file
     * @return
     */
    public String getdbtableName(File file){
        //获得库名
        String dbName = file.getParentFile().getParentFile().getName();
        StringBuilder sb = new StringBuilder();
        //获得表名
        String tableName = file.getParentFile().getName();
        //匹配库名是否符合正则
        Matcher matcher = pattern.matcher(dbName);
        if (matcher.find() && pattern.matcher(tableName).find()) {
            String[] dbSplits = dbName.split("_");
            String[] tableSplits = tableName.split("_");
            for (int i = 0; i < dbSplits.length; i++) {
                if (i != dbSplits.length - 1) {
                    if (i != dbSplits.length - 2) {
                        sb.append(dbSplits[i]).append("_");
                    } else {
                        //反正就是最后拼接出来的库名没有库号
                        sb.append(dbSplits[i]);
                    }
                }
            }


            for (int i = 0; i < tableSplits.length; i++) {
                if (i != tableSplits.length - 1) {
                    if (i != tableSplits.length - 2) {
                        sb.append(tableSplits[i]).append("_");
                    } else {
                        //最后获取表名且不带序号
                        sb.append(tableSplits[i]);
                    }
                }
            }
        } else if (pattern.matcher(tableName).find()) {
            sb.append(dbName);
            String[] tableSplits = tableName.split("_");
            for (int i = 0; i < tableSplits.length; i++) {
                if (i != tableSplits.length - 1) {
                    if (i != tableSplits.length - 2) {
                        sb.append(tableSplits[i]).append("_");
                    } else {
                        sb.append(tableSplits[i]);
                    }
                }
            }
        } else {
            sb.append(dbName).append(tableName);
        }
        //最后返回，dbName+tableName
        //db_enterpriset_enterprise
        return  new String(sb);
    }






    /**
     * 从文件名中提取日期
     *
     * @param file ：file-2019-08-08-0000.log
     * @return
     */
    private String getDateString(File file) {
        String[] fileNameArrayWithLine = file.getName().split("-");
        String date = "";
        if (fileNameArrayWithLine.length >= 3) {
            date = fileNameArrayWithLine[1] + "-" + fileNameArrayWithLine[2] + "-" + fileNameArrayWithLine[3];
        }
        if (!isValidDate(date)) {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            date = df.format(new Date());
        }
        return date;
    }

    //获得输出文件名
    private String getOutputFileName(File file, String originTable) {
        //输出文件名由，库表16进制+文件名称
        //ff_file-2020-07-24-0002.log
        return originTable + "_" + file.getName();
    }

    //获取下载路径下的表名
    private String getTableName(File file) {
        //t_last_industry_0，分割成数组
        String[] tables = file.getParentFile().getName().split("_");
        if (tables.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        String lastIndex = tables[tables.length - 1];
        // 有的表名不是以t_ 开头,或者以分表号码结尾，组装表名的时候数组位置不同
        //是以数字结尾，即分表
        if (isInteger(lastIndex)) {
            int index = Integer.parseInt(lastIndex);
            //结尾的数字没有问题
            if ((index >= MIN_INDEX) && (index <= MAX_INDEX)) {
                //如果是以t开头
                if (TABLE_PREFIX.equals(tables[0])) {
                    //t_other_information_15,抛弃t，最后得到的结果是other_information，i < tables.length - 1，即抛弃最后的数字结尾
                    for (int i = 1; i < tables.length - 1; i++) {
                        sb.append(tables[i]).append("_");
                    }
                } else {
                    //other_information_15
                    for (int i = 0; i < tables.length - 1; i++) {
                        sb.append(tables[i]).append("_");
                    }
                }
            } else {
                getTableNameViaArray(tables, sb);
            }
        } else {
            getTableNameViaArray(tables, sb);
        }
        //反正最后的返回结果就是other_information
        return sb.substring(0, sb.length() - "_".length());
    }

    private void getTableNameViaArray(String[] tables, StringBuilder sb) {
        if (TABLE_PREFIX.equals(tables[0])) {
            //t_other_information
            for (int i = 1; i < tables.length; i++) {
                sb.append(tables[i]).append("_");
            }
        } else {
            //other_information
            for (String table : tables) {
                sb.append(table).append("_");
            }
        }
    }


    private static boolean isValidDate(String str) {
        boolean convertSuccess = true;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            format.setLenient(false);
            format.parse(str);
        } catch (Exception e) {
            convertSuccess = false;
        }
        return convertSuccess;
    }

}
