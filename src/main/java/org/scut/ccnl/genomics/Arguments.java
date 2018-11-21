package org.scut.ccnl.genomics;

import org.apache.commons.cli.CommandLine;
import org.apache.ibatis.io.Resources;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/*
    配置参数，能够从指定路径中获取
 */
public class Arguments {
    final protected static Logger logger = Logger.getLogger(Arguments.class);

    public static boolean OVERLAP = true;
//    public static boolean OVERLAP = false;

    public static boolean OBJECT_POOL = false;
//    public static boolean OBJECT_POOL = false;

    //    public static boolean FROM_LOCAL = true;
    public static boolean FROM_LOCAL = false;
    public static String FASTA_PREFIX ;
    //    public static String FASTA_PREFIX = "/data/home/wzz/temp/hg19";
//    public static String FASTA_PREFIX ="/SHARE/wzz_workstation/index/hg19";
    public static String FASTA_INDEX = FASTA_PREFIX+".fasta.fai";
    public static String FASTA_DICT = FASTA_PREFIX+".dict";
    public static String FASTA = FASTA_PREFIX+".fasta";

    public static int SHARD_SIZE = 10000;
    public static int SUBLIST_OVERLAP_SIZE = 500;
//    public static int INPUT_FORMAT_OVERLAP_SIZE = 0;
    public static int INPUT_FORMAT_OVERLAP_SIZE = 1024;

    public static Set<Integer> additionSet = new HashSet<>();

    // 是否对最后n个分块进行切分
    public static volatile boolean ADDITION_SPLIT = false;
    //    public static boolean ADDITION_SPLIT = false;
    // n个分块中n为多少
    public static volatile double CHANGE_SPLITS_SIZE = 64;
    // 每个分块切分多个
    public static volatile int ADDITION_EACH_SPLIT_SIZE = 8;

    public static volatile int PARTITION_TIME_MAX = 2000;
    public static volatile int PARTITION_TIME_AVG = 200;

    public static volatile int WORKER_NUM = 8;

    public static volatile int CHANGE_PARTITION_NUM = (PARTITION_TIME_MAX/PARTITION_TIME_AVG)*(WORKER_NUM-1);

    public static volatile Set<Integer> PARTITIONS_TO_CHANGE;

    public static volatile int VALID_PARTITION_NUM = 0;

    public static volatile int LAST_PARTITION_NUM_TO_CHANGE = 10;

    public static volatile int PARTITION_SIZE = 0;

    public static String HBASE_REFERENCE = "Reference";

    public static String HBASE_DBSNP = "dbsnp";

    public static String HBASE_DBSNP_FAMILY_POS = "position";
    // fasta的来源:hbase,mongo,hdfs,nfs
    public static String REFERENCE_DB = "nfs";
    // dbsnp的来源:hbase,mysql,none
    public static String DBSNP_DB = "none";

    public static String IMPALA_URL = "jdbc:hive2://192.168.0.10:21050/;auth=noSasl";
    public static String IMPALA_DBSNP = "dbsnp";

    public static String MYSQL_URL = "";
    public static String MYSQL_USERNAME="";
    public static String MYSQL_PASSWORD="";

    public static String DBSNP_PATH = "";
    public static boolean DBSNP_CACHE = false;

    public static volatile List<Tuple2<Integer,Long>> arinfo;
    public static volatile Long ar_avg;



    // 默认从Resources中读取配置文件
    public static void init(){
        Properties prop = new Properties();
        try (InputStream in = Resources.getResourceAsStream("constants.properties")) {
            prop.load(in);
            setProp(prop);

        }catch (Exception e){
            e.printStackTrace();
        }
        // TODO: 2017/5/25 only for local test
        if(FROM_LOCAL){
            FASTA_PREFIX = "D:\\genomic\\hg19";
            FASTA_INDEX = FASTA_PREFIX+".fasta.fai";
            FASTA_DICT = FASTA_PREFIX+".dict";
            FASTA = FASTA_PREFIX+".fasta";
        }
    }

    public static void init(CommandLine commandLine){
        if(commandLine.hasOption("dc"))
            DBSNP_CACHE = true;
        if(commandLine.hasOption("dt"))
            DBSNP_DB = commandLine.getOptionValue("dt").toLowerCase();
        if(commandLine.hasOption("dp"))
            DBSNP_PATH = commandLine.getOptionValue("dp").toLowerCase();
    }

    // 从参数路径中读取配置文件
    public static void init(String filename){

        Properties prop = new Properties();

        try (InputStream in = new FileInputStream(new File(filename))) {
            prop.load(in);
            setProp(prop);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void setProp(Properties prop){
        for(Map.Entry<Object,Object> entry : prop.entrySet()){
            String key = (String) entry.getKey();
            switch (key){
                case "FROM_LOCAL":
                    FROM_LOCAL = entry.getValue().toString().toLowerCase().equals(Constants.STR_TRUE);
                    break;
                case "ADD_BLOCK":
                    String[] strs = entry.getValue().toString().split(",");
                    for(String str: strs){
                        additionSet.add(Integer.valueOf(str));
                    }
                    break;
                case "DBSNP_CACHE":
                    DBSNP_CACHE = entry.getValue().toString().toLowerCase().equals(Constants.STR_TRUE);
                    break;
                case "FASTA_PREFIX":
                    FASTA_PREFIX = entry.getValue().toString();
                    FASTA_INDEX = FASTA_PREFIX+".fasta.fai";
                    FASTA_DICT = FASTA_PREFIX+".dict";
                    FASTA = FASTA_PREFIX+".fasta";
                    break;
                case "ADDITION_SPLIT":
                    ADDITION_SPLIT = entry.getValue().toString().toLowerCase().equals(Constants.STR_TRUE);
                    break;
                case "CHANGE_SPLITS_SIZE":
                    CHANGE_SPLITS_SIZE = Double.valueOf(entry.getValue().toString());
                    break;
                case "INPUT_FORMAT_OVERLAP_SIZE":
                    INPUT_FORMAT_OVERLAP_SIZE = Integer.valueOf(entry.getValue().toString());
                    break;
                case "ADDITION_EACH_SPLIT_SIZE":
                    ADDITION_EACH_SPLIT_SIZE = Integer.valueOf(entry.getValue().toString());
                    break;
                case "PARTITION_SIZE":
                    String partition_size_str = entry.getValue().toString().toLowerCase();
                    if(partition_size_str.endsWith("m")){
                        PARTITION_SIZE = 1024 * 1024 *
                                Integer.valueOf(partition_size_str.substring(0,partition_size_str.length()-1));
                    }else {
                        PARTITION_SIZE = Integer.valueOf(entry.getValue().toString());
                    }
                    break;
                case "DBSNP_PATH":
                    DBSNP_PATH = entry.getValue().toString();
                    break;
                case "DBSNP_DB":
                    DBSNP_DB = entry.getValue().toString().toLowerCase();
                    break;
                case "IMPALA_DBSNP":
                    IMPALA_DBSNP = entry.getValue().toString();
                    break;
                case "MYSQL_URL":
                    MYSQL_URL = entry.getValue().toString();
                    break;
                case "MYSQL_USERNAME":
                    MYSQL_USERNAME = entry.getValue().toString();
                    break;
                case "MYSQL_PASSWORD":
                    MYSQL_PASSWORD = entry.getValue().toString();
                    break;
                case "IMPALA_URL":
                    IMPALA_URL = entry.getValue().toString();
                default:
                    logger.warn("error init constants "+key);

            }
        }
    }


}
