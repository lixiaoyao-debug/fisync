package com.qixin.fisync;

import com.qixin.fisync.parse.IncreaseParse;
import com.qixin.fisync.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by Dream on 2020/2/12 14:09
 *
 * @author Dream
 */
public class AppMain {
    //获取AppMain这个类的日志
    private static Logger logger = LoggerFactory.getLogger(AppMain.class);
    //创建一个Map存储错误报告
    private static HashMap<Throwable, Long> errorReport = new HashMap<>();
    //创建一个List存储不用的key
    private static ArrayList<String> unUseKey = new ArrayList<>();

    public static void main(String[] args) {
        //输入数据内容的第一个参数,配置文件的路径是由传入的第一个参数来决定的,即config.properties的配置文件路径
        String propertiesPath = args[0];
        //将传入的第一个参数设置为系统配置
        System.setProperty("propertiesPath", propertiesPath);
        //PropertyUtil并加载这个配置
        PropertyUtil.loadProps(propertiesPath);
        //创建一个单线程的执行器
        ExecutorService service = Executors.newSingleThreadExecutor();

        try {
            //线程睡5s
            Thread.sleep(5000);
            //通过PropertyUtil获取各种配置路径
            String tableConfigPath = PropertyUtil.getProperty("tableConfigPath");
            String outputPath = PropertyUtil.getProperty("outputPath");
            String downLoadPath = PropertyUtil.getProperty("downLoadPath");
            String deletePath = PropertyUtil.getProperty("deletePath");
            //获取睡眠时间的配置文件参数
            int sleepTime = Integer.parseInt(PropertyUtil.getProperty("parse_interval"));
            File file = new File(tableConfigPath);
            //设置表属性
            FileUtil.setTableFields(file);

            //解析输出路径
            IncreaseParse increaseParse = new IncreaseParse(outputPath);
            while (true) {
                //获取下载路径下所有文件（没有正在被写的文件）
                List<File> allFiles = FileUtil.getAllFiles(downLoadPath);

                //创建一个HashMap用来做校验
                HashMap<String, String> checkMap = new HashMap<>();
                for (File file1 : allFiles) {
                    String dbtableName = increaseParse.getdbtableName(file1);
                    String fileName = file1.getName();
                    String preFileName = checkMap.get(dbtableName);
                    //preFileName>=fileName,证明文件顺序不对
                    if (preFileName != null && preFileName.compareTo(fileName)>=0) {
                        logger.info("File Order Error!!!");
                        System.exit(1);
                    }else{
                        checkMap.put(dbtableName,fileName);
                    }

                    //遍历这个文件，把这个文件解析出来，再传到待删除路径
                    increaseParse.parseJsonFile(file1, deletePath);
                }
                //打印日志信息
                String msg = ReportUtil.heartbeat("heartbeat from bigdata parse");
                logger.info("info from heartbeat is {}", msg);
                Thread.sleep(sleepTime * 1000);
            }
        } catch (InterruptedException e) {
            logger.error("thread sleep error:", e);
        } catch (Exception e) {
            Long aLong = errorReport.get(e);
            Long now = System.currentTimeMillis();
            if (aLong == null || (now - aLong > 12 * 60 * 60 * 1000)) {
                errorReport.put(e, now);
//                ReportUtil.reportLog(Constants.SERIOUS_ERROR, "APP-MAIN-60" + e.toString());
            }
            logger.error("parse stop with error:", e);
        }
    }
}
