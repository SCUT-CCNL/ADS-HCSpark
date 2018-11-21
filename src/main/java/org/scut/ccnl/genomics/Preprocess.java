package org.scut.ccnl.genomics;



import hdfs.jsr203.HadoopFileSystem;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;
import org.seqdoop.hadoop_bam.GATKSAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.IOException;
import java.util.List;

public class Preprocess {
    public static void preprocess(CommandLine commandLine) throws Exception {
        if(!SparkHC.checkPram(commandLine)) return;

        String readFileName = commandLine.getOptionValue("i");

        SparkConf sparkConf;
        if(Arguments.FROM_LOCAL)
            sparkConf = new SparkConf().setAppName("SparkHC").setMaster("local[1]").
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
                    set("spark.kryo.registrator", GATKRegistrator.class.getCanonicalName());
        else
            sparkConf = new SparkConf().setAppName("SparkHC").
                    set("spark.driver.extraJavaOptions","-XX:MaxMetaspaceSize=512m").
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
//        globalArgument.setSampleList(VariantCaller.getHeader(readFileName,conf));
        final Broadcast<GlobalArgument> globalArgumentBroadcast = jsc.broadcast(globalArgument);

        JavaPairRDD<LongWritable, SAMRecordWritable> reads_rdd = null;

        reads_rdd = jsc.newAPIHadoopFile(
                readFileName, GATKSAMInputFormat.class,
                LongWritable.class,SAMRecordWritable.class,conf);

        JavaRDD<GATKSAMRecord> samRecordRDD =  reads_rdd.map(x->new GATKSAMRecord(x._2().get()));

        JavaRDD<String> preInfoRdd = samRecordRDD.mapPartitionsWithIndex((id, gatksamRecordIterator) ->{
//            return VariantCaller.getCoverage(id,gatksamRecordIterator,globalArgumentBroadcast.value());
            return VariantCaller.getPreprocessInfo(id,gatksamRecordIterator);
        },false);

        List<String> preInfo = preInfoRdd.collect();

        String output = null;

        //指定输出
        if(commandLine.hasOption("o")){
            output = commandLine.getOptionValue("o");
        }else{
            output = readFileName + Constants.HC_INDEX_SUFFIX;
        }

        FileSystem hdfs;
        if(output.startsWith("file:///"))
            hdfs = FileSystem.getLocal(conf);
        else
            hdfs = FileSystem.get(conf);

        FSDataOutputStream fsDataOutputStream = hdfs.create(new Path(output));
        fsDataOutputStream.write(("#"+conf.get("mapreduce.input.fileinputformat.split.maxsize")+"\n").getBytes());
        for(String line : preInfo) {
            fsDataOutputStream.write((line+"\n").getBytes());
        }
        fsDataOutputStream.close();




    }

}
