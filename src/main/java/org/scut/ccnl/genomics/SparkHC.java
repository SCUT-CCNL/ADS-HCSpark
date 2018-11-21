package org.scut.ccnl.genomics;



import htsjdk.variant.variantcontext.*;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;
import org.scut.ccnl.genomics.io.DBsnpTools;
import org.scut.ccnl.genomics.io.KuduSample;
import org.scut.ccnl.genomics.io.dbsnp.BuildHBaseDbsnp;
import org.seqdoop.hadoop_bam.GATKSAMInputFormat;
import org.seqdoop.hadoop_bam.GATKSAMPreprocessInputFormat;
import org.seqdoop.hadoop_bam.GATKSAMPriorityInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Usage: SparkHC [slices]
 */
public class SparkHC {
    final static Logger logger = Logger.getLogger(SparkHC.class);


    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        // Create a Parser
        CommandLineParser parser = new BasicParser( );
        Options options = new Options( );
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("t", "tool", true, "Tool to execute" );
        options.addOption("i", "input", true, "InputFile path" );
        options.addOption("o", "output", true, "OutputFile path");
        options.addOption("c", "conf", true, "Configration file path");
        options.addOption("s", "sort", false, "Sort vcf");
        options.addOption("p", "preprocess", false, "Preproceess");
        options.addOption("pf", "preprocessFilePath", true, "PreproceessFilePath");
        options.addOption("dc", "dbsnp-cache", false, "dbsnp-cache");
        options.addOption("dt", "dbsnp-type", false, "dbsnp-type");
        options.addOption("dp", "dbsnp-path", false, "dbsnp-path");
        // Parse the program arguments
        CommandLine commandLine = parser.parse( options, args );

        // 根据配置文件进行初始化
        if(commandLine.hasOption("c")){
            String name = commandLine.getOptionValue("c");
            Arguments.init(name);
        }else {
            logger.info("init arguments by build-in properties");
            Arguments.init();
        }

        Arguments.init(commandLine);

        switch (commandLine.getOptionValue("t","null")){
            case Constants.STR_HC:
                SparkHC.runSpark(commandLine);
                break;
            case Constants.STR_BUILD_MYSQL_DBSNP:
                DBsnpTools.buildDbsnp(commandLine);
                break;
            case Constants.STR_BUILD_KUDU_DBSNP:
                KuduSample.main(args);
                break;
            case Constants.STR_BUILD_HBASE_DBSNP:
                BuildHBaseDbsnp.buildDbsnp(commandLine);
                break;
            case Constants.STR_BUILD_PREPROCESS:
                Preprocess.preprocess(commandLine);
                break;
            case "vcfmerge":
                VcfMerge.simpleMerge(commandLine);
                break;
            default:
                logger.info("-t is wrong");
        }
        long end = System.currentTimeMillis();
        logger.info("done:"+(end-start)/1000);
        System.out.println("\n done:"+(end-start)/1000);

    }
    //运行HC的基本参数
    public static boolean checkPram(CommandLine commandLine){
        if(!commandLine.hasOption("i")){
            logger.error("-i is wrong");
            return false;
        }

        return true;
    }

    public static void runSpark(CommandLine commandLine) throws Exception {
        if(!checkPram(commandLine)) return;

        String readFileName = commandLine.getOptionValue("i");


        SparkConf sparkConf;
        if(Arguments.FROM_LOCAL)
            sparkConf = new SparkConf().setAppName("SparkHC").setMaster("local[1]").
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
                    set("spark.kryo.registrator", GATKRegistrator.class.getCanonicalName());
        else
            sparkConf = new SparkConf().setAppName("SparkHC").
                    set("spark.driver.extraJavaOptions","-XX:MetaspaceSize=1024M").
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
                    set("spark.kryo.registrator", GATKRegistrator.class.getCanonicalName());


        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Configuration conf = jsc.hadoopConfiguration();

        if(Arguments.PARTITION_SIZE > 0){
            conf.set("mapreduce.input.fileinputformat.split.maxsize",String.valueOf(Arguments.PARTITION_SIZE));
            conf.set("mapreduce.input.fileinputformat.split.minsize",String.valueOf(Arguments.PARTITION_SIZE));
        }

        //广播公共变量
        GlobalArgument globalArgument = new GlobalArgument(true );
        globalArgument.setSampleList(VariantCaller.getHeader(readFileName,conf));
        // TODO: 2017/5/26 需要commandLine对象对GlobalArgument进行初始化
        globalArgument.setOutput(commandLine.getOptionValue("o",Constants.STR_EMPTY));
        if(commandLine.hasOption("s"))
            globalArgument.sort = true;

        final Broadcast<GlobalArgument> globalArgumentBroadcast = jsc.broadcast(globalArgument);

        JavaPairRDD<LongWritable, SAMRecordWritable> reads_rdd = null;

        if(commandLine.hasOption("p")){
            long preprocess_start = System.currentTimeMillis();
            String idxFile = null;
            if(commandLine.hasOption("pf"))
                idxFile = commandLine.getOptionValue("pf");
            else
                idxFile = readFileName+Constants.HC_INDEX_SUFFIX;
            System.out.println("hcidx:"+idxFile);

            JavaRDD<String> hcIdx = jsc.textFile(idxFile);

            VariantCaller.setPreprocessInfo(hcIdx.collect());

            System.out.println("预处理耗时:"+(System.currentTimeMillis()-preprocess_start)/1000);

            reads_rdd = jsc.newAPIHadoopFile(
                    readFileName, GATKSAMPriorityInputFormat.class,
                    LongWritable.class,SAMRecordWritable.class,conf);

        }else {
            reads_rdd = jsc.newAPIHadoopFile(
                    readFileName, GATKSAMInputFormat.class,
                    LongWritable.class,SAMRecordWritable.class,conf);
        }

        JavaRDD<GATKSAMRecord> samRecordRDD =  reads_rdd.map(x->new GATKSAMRecord(x._2().get()));
        final LongAccumulator dbsnpTimeAccumulator = jsc.sc().longAccumulator();

        //直接进行编译检测
        JavaRDD<VariantContext> vcfrdd = samRecordRDD.mapPartitionsWithIndex((id,gatksamRecordIterator) ->{
            return VariantCaller.getVariantContexts(id,
                    gatksamRecordIterator, globalArgumentBroadcast.getValue(),dbsnpTimeAccumulator);
        },false);

        VariantCaller.printVariantContexts(jsc,conf,vcfrdd,globalArgument,commandLine);
        System.out.println("dbsnp耗时:" + TimeUnit.NANOSECONDS.toSeconds(dbsnpTimeAccumulator.value()));
        jsc.stop();
    }


}
