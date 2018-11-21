package org.scut.ccnl.genomics;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.gatk.engine.io.DirectOutputTracker;
import org.broadinstitute.gatk.engine.io.stubs.VariantContextWriterStub;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWithHeader;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by shedfree on 2018/3/23.
 */
public class VcfMerge {

    public static void simpleMerge(CommandLine commandLine) throws IOException {
        String readFileName = commandLine.getOptionValue("i");
        String outputFile = commandLine.getOptionValue("o");
        Path path = new Path(outputFile);

        SparkConf sparkConf;
        if(Arguments.FROM_LOCAL)
            sparkConf = new SparkConf().setAppName("SparkHC").setMaster("local[1]");
        else
            sparkConf = new SparkConf().setAppName("SparkHC").
                    set("spark.driver.extraJavaOptions","-XX:MetaspaceSize=1024M");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Configuration conf = jsc.hadoopConfiguration();

        JavaPairRDD<LongWritable, VariantContextWritable> rdd2 = jsc.newAPIHadoopFile(readFileName, VCFInputFormat.class, LongWritable.class, VariantContextWritable.class,conf);

        JavaRDD<VariantContext> variantContextJavaRDD = rdd2.map(v1 -> v1._2().get());

        List<VariantContext> list = new ArrayList<>(variantContextJavaRDD.collect());

        VCFHeader header = ((VariantContextWithHeader)list.get(0)).getHeader();
        list.sort(header.getVCFRecordComparator());

        FileSystem fs = null;//path.getFileSystem(conf);

        OutputStream outputStream;
        fs = FileSystem.getLocal(conf);
        // 如果文件存在，就先删除
        if(fs.exists(path)) {
            fs.delete(path, true);
        }
        outputStream = fs.create(path);

        VariantContextWriterStub vcfWriter = new VariantContextWriterStub(null,outputStream, Arrays.asList());
        DirectOutputTracker outputTracker = new DirectOutputTracker();
        outputTracker.addOutput(vcfWriter);
        vcfWriter.writeHeader(header);

        VariantContext last = null;
        for(VariantContext current : list){
            if(last!=null && last.getStart()==current.getStart() && last.getContig().equals(current.getContig()))
                continue;
            vcfWriter.add(current);
            last = current;
        }
        outputStream.close();
        System.out.println();


    }
}
