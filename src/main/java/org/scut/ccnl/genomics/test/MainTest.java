package org.scut.ccnl.genomics.test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import htsjdk.samtools.util.PeekableIterator;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.CloseableTribbleIterator;
import htsjdk.tribble.FeatureReader;
import htsjdk.tribble.TribbleIndexedFeatureReader;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;
import org.junit.Test;
import org.scut.ccnl.genomics.*;
import org.scut.ccnl.genomics.io.NativeLibraryLoader;
import org.scut.ccnl.genomics.io.dbsnp.*;
import org.scut.ccnl.genomics.io.fasta.DistributedFastaSequenceFile;
import org.scut.ccnl.genomics.io.fasta.HDFSFasta;
import org.scut.ccnl.genomics.io.fasta.HbaseFasta;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.GATKSAMInputFormat;
import org.seqdoop.hadoop_bam.GATKSAMPriorityInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MainTest {
    @Test
    public void mysqlConnectTest(){
        System.setProperty(Constants.MYSQL_URL, "jdbc:mysql://cu01:3306/dbsnp");
        System.setProperty(Constants.MYSQL_USERNAME, "root");
        System.setProperty(Constants.MYSQL_PASSWORD, "spacedb");

        MysqlVCFCodec vcfCodec = new MysqlVCFCodec();
        System.out.println();
    }

    @Test
    public void test(){
//        try {
//            //验证两种方式获取的是否相同
//            IndexedFastaSequenceFile indexedFastaSequenceFile = new IndexedFastaSequenceFile(new File("D:\\genomic\\hg19.fasta"));
//            String a = indexedFastaSequenceFile.getSubsequenceAt("chr2",15640,16110).getBaseString();
//            String b = MongoFasta.mongoFasta.getSubsequenceAt("chr2",15640,16110).getBaseString();
//            System.out.println(a.equals(b));
//            System.out.println(a);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
    }
    @Test
    public void test2(){
        try {
            //验证两种方式获取的是否相同
            IndexedFastaSequenceFile indexedFastaSequenceFile = new IndexedFastaSequenceFile(new File(Arguments.FASTA));
            String a = indexedFastaSequenceFile.getSubsequenceAt("chr2",45000,62000).getBaseString();
            String b = HbaseFasta.hbaseFasta.getSubsequenceAt("chr2",45000,62000).getBaseString();
            System.out.println(a.equals(b));
            System.out.println(a);
            System.out.println("-------------------------------");
            System.out.println(b);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void test3(){
        try {
            //验证两种方式获取的是否相同
            IndexedFastaSequenceFile indexedFastaSequenceFile = new IndexedFastaSequenceFile(new File(Arguments.FASTA));
            String a = indexedFastaSequenceFile.getSubsequenceAt("chr2",45000,62000).getBaseString();
            String b = HDFSFasta.hdfsFasta.getSubsequenceAt("chr2",45000,62000).getBaseString();
            System.out.println(a.equals(b));
//            System.out.println(a);
            System.out.println("-------------------------------");
//            System.out.println(b);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCRC(){
//        int a = Utils.getCRCHash("chr1-111");
//        int b = Utils.getCRCHash("chr1-222");
//        int c = compatHashingAlg2("chr1-111");
//        int d = compatHashingAlg2("chr1-222");
        System.out.println("------");
    }

    @Test
    public void deserializeTest() throws FileNotFoundException {
        Kryo kryo = new Kryo();
        Input input = new Input(new FileInputStream("E:\\kryo.txt"));
        List l = (ArrayList<GATKSAMRecord>)kryo.readClassAndObject(input);
        System.out.println(l.size());

    }

    @Test
    public void loadLib() throws Exception{
        NativeLibraryLoader.load(null, "libVectorLoglessPairHMM.so");

    }
    @Test
    public void testImpala(){
        GenomeLocParser parser = new GenomeLocParser(
                DistributedFastaSequenceFile.getSequenceDictionary("D:/genomic/hg19.dict"));
        parser.setMRUCachingSAMSequenceDictionary();
        Arguments.init();
        System.setProperty(Constants.MYSQL_URL, Arguments.MYSQL_URL);
        System.setProperty(Constants.MYSQL_USERNAME, Arguments.MYSQL_USERNAME);
        System.setProperty(Constants.MYSQL_PASSWORD, Arguments.MYSQL_PASSWORD);
        System.setProperty(Constants.IMPALA_URL,"jdbc:hive2://192.168.0.9:21053/;auth=noSasl");
//        System.setProperty(Constants.IMPALA_URL,"jdbc:hive2://192.168.0.10:21050/;auth=noSasl");
        ImpalaVCFCodec impalaVCFCodec = new ImpalaVCFCodec("dbsnp");
//        MysqlVCFCodec mysqlVCFCodec = new MysqlVCFCodec();


        RefMetaDataTracker a = impalaVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
//        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
        System.out.println("");



    }

    @Test
    public void testLoopTime(){
        long start = System.currentTimeMillis();
        double sum = Math.random();
        // 5千万的耗时80~100 mill
        for (int i = 0; i < 50000000; i++) {
            sum+=i;
        }

        long end = System.currentTimeMillis();
//        System.out.println(sum);
        System.out.println(end-start);


    }

    @Test
    public void createKudu() throws KuduException {


        List<ColumnSchema> columns = new ArrayList<>(8);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("chrom", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("pos", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ref", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("alt", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("rs_id", Type.STRING)
                .build());

        columns.add(new ColumnSchema.ColumnSchemaBuilder("qual", Type.STRING)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("filter", Type.STRING)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("info", Type.STRING)
                .build());

        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("chrom");

        String KUDU_MASTER = "192.168.0.10:7051";
        String tableName = "dbsnp_kudu";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        Schema schema = new Schema(columns);
        client.createTable(tableName, schema,
                new CreateTableOptions().setRangePartitionColumns(rangeKeys));

    }

    @Test
    public void deleteKudu() throws KuduException {
        String KUDU_MASTER = "192.168.0.10:7051";
        String tableName = "dbsnp_kudu";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        client.deleteTable(tableName);


    }

    @Test
    public void testKudu(){
        GenomeLocParser parser = new GenomeLocParser(
                DistributedFastaSequenceFile.getSequenceDictionary("D:/genomic/hg19.dict"));
        parser.setMRUCachingSAMSequenceDictionary();
        Arguments.init();
        System.setProperty(Constants.MYSQL_URL, Arguments.MYSQL_URL);
        System.setProperty(Constants.MYSQL_USERNAME, Arguments.MYSQL_USERNAME);
        System.setProperty(Constants.MYSQL_PASSWORD, Arguments.MYSQL_PASSWORD);
        System.setProperty(Constants.IMPALA_URL,"jdbc:hive2://192.168.0.10:21053/;auth=noSasl");
//        System.setProperty(Constants.IMPALA_URL,"jdbc:hive2://192.168.0.10:21050/;auth=noSasl");
//        KuduVCFCodec kuduVCFCodec = new KuduVCFCodec(null);
        ImpalaVCFCodec impalaVCFCodec = new ImpalaVCFCodec("dbsnp");

//        RefMetaDataTracker a = kuduVCFCodec.parserLocation(parser.createGenomeLoc("chr1",0,209290656),parser);
//        RefMetaDataTracker a = kuduVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
        RefMetaDataTracker b = impalaVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
        System.out.println("");

    }

    @Test
    public void testInterval(){
        SparkConf sparkConf = new SparkConf().setAppName("SparkHC").setMaster("local[1]").
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
                    set("spark.kryo.registrator", GATKRegistrator.class.getCanonicalName());

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Configuration conf = jsc.hadoopConfiguration();
        conf.set("hadoopbam.bam.intervals","chr21:10710000-10712000");

        JavaPairRDD<LongWritable, SAMRecordWritable> reads_rdd = jsc.newAPIHadoopFile(
                "D:\\genomic\\chr21_1_4.bam",BAMInputFormat.class,
                LongWritable.class,SAMRecordWritable.class,conf);

        System.out.println(reads_rdd.collect());

    }

    @Test
    public void testPreprocessDevide(){
//        Arguments.PARTITIONS_TO_CHANGE = Arrays.asList(1);
//        Arguments.LAST_PARTITION_NUM_TO_CHANGE = 2;
//
//        SparkConf sparkConf = new SparkConf().setAppName("SparkHC").setMaster("local[1]").
//                set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
//                set("spark.kryo.registrator", GATKRegistrator.class.getCanonicalName());
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        Configuration conf = jsc.hadoopConfiguration();
//
//        JavaPairRDD<LongWritable, SAMRecordWritable> reads_rdd = null;
//
//        reads_rdd = jsc.newAPIHadoopFile(
//                "D:\\genomic\\chr21.114m.bam", GATKSAMPriorityInputFormat.class,
//                LongWritable.class,SAMRecordWritable.class,conf);
//
//        System.out.println(reads_rdd.count());

    }

    @Test
    public void testVCFindex() throws IOException {
        FeatureReader<VariantContext> reader =  AbstractFeatureReader.getFeatureReader("D:\\genomic\\dbsnp_135.hg19.modify.vcf",new VCFCodec(),true);

        CloseableTribbleIterator<VariantContext> iterator = reader.query("chr2",20008,30100);
        PeekableIterator<VariantContext> peekableIterator = new PeekableIterator<>(iterator);
        iterator.close();
        System.out.println();
    }

    @Test
    public void testNfsVcf(){
        GenomeLocParser parser = new GenomeLocParser(
                DistributedFastaSequenceFile.getSequenceDictionary("D:/genomic/hg19.dict"));
        parser.setMRUCachingSAMSequenceDictionary();
        Arguments.init();

        System.setProperty(Constants.MYSQL_URL, Arguments.MYSQL_URL);
        System.setProperty(Constants.MYSQL_USERNAME, Arguments.MYSQL_USERNAME);
        System.setProperty(Constants.MYSQL_PASSWORD, Arguments.MYSQL_PASSWORD);
        MysqlVCFCodec mysqlVCFCodec = new MysqlVCFCodec();
        NfsVCFCodec nfsVCFCodec = new NfsVCFCodec("D:\\genomic\\dbsnp\\dbsnp_135.hg19.modify.vcf");

        RefMetaDataTracker a = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
//        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr21",9427874,9528129),parser);
//        RefMetaDataTracker a = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr2",9427874,9528129),parser);
//        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr2",9427874,9528129),parser);

//        RefMetaDataTracker b = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209290657,209300656),parser);
//        RefMetaDataTracker a = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
//        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);

        System.out.println();
    }

    @Test
    public void testHdfsVcf(){
        GenomeLocParser parser = new GenomeLocParser(
                DistributedFastaSequenceFile.getSequenceDictionary("D:/genomic/hg19.dict"));
        parser.setMRUCachingSAMSequenceDictionary();
        Arguments.init();

        System.setProperty(Constants.MYSQL_URL, Arguments.MYSQL_URL);
        System.setProperty(Constants.MYSQL_USERNAME, Arguments.MYSQL_USERNAME);
        System.setProperty(Constants.MYSQL_PASSWORD, Arguments.MYSQL_PASSWORD);
//        MysqlVCFCodec mysqlVCFCodec = new MysqlVCFCodec();
        NfsVCFCodec nfsVCFCodec = new NfsVCFCodec("hdfs:///user/cloudera/dbsnp_135.hg19.modify.vcf");

        RefMetaDataTracker a = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
//        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr21",9427874,9528129),parser);
//        RefMetaDataTracker a = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr2",9427874,9528129),parser);
//        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr2",9427874,9528129),parser);

//        RefMetaDataTracker b = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209290657,209300656),parser);
//        RefMetaDataTracker a = nfsVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
//        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);

        System.out.println();
    }

    @Test
    public void testCacheVcf(){
        GenomeLocParser parser = new GenomeLocParser(
                DistributedFastaSequenceFile.getSequenceDictionary("D:/genomic/hg19.dict"));
        parser.setMRUCachingSAMSequenceDictionary();
        Arguments.init();

        GlobalArgument globalArgument = new GlobalArgument();
        globalArgument.DBSNP_PATH = "D:\\genomic\\dbsnp_135.hg19.modify.vcf";
        globalArgument.DBSNP_DB = "nfs";

        VCFDBCodec vcfdbCodec = VCFDBCacheCodecFactory.getVCFDBCodec(globalArgument);
        RefMetaDataTracker a = vcfdbCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
        RefMetaDataTracker b = vcfdbCodec.parserLocation(parser.createGenomeLoc("chr1",209290657,209300656),parser);
        RefMetaDataTracker c = vcfdbCodec.parserLocation(parser.createGenomeLoc("chr1",209390752,209390852),parser);
//        RefMetaDataTracker c = vcfdbCodec.parserLocation(parser.createGenomeLoc("chr1",209390552,209390752),parser);
        System.out.println();

    }

    @Test
    public void hbaseDbsnp(){
        GenomeLocParser parser = new GenomeLocParser(
                DistributedFastaSequenceFile.getSequenceDictionary("D:/genomic/hg19.dict"));
        parser.setMRUCachingSAMSequenceDictionary();
        Arguments.init();

        HbaseVCFCodec vcfCodec = new HbaseVCFCodec();
//        RefMetaDataTracker b = vcfCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
        RefMetaDataTracker b = vcfCodec.parserLocation(parser.createGenomeLoc("chr1",9978,10087),parser);
        KuduVCFCodec kuduVCFCodec = new KuduVCFCodec(null);
        RefMetaDataTracker a = kuduVCFCodec.parserLocation(parser.createGenomeLoc("chr1",9978,10087),parser);
//        KuduVCFCodec kuduVCFCodec = new KuduVCFCodec(null);
//        RefMetaDataTracker a = kuduVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);

        System.out.println();

    }

    @Test
    public void fileName(){
        String file = "/data/wzz/hg19.fasta";
        System.out.println(file.substring(0,file.lastIndexOf(".")));
        System.out.println();

    }


    @Test
    public void testSetPreprocess() throws Exception {

        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\genomic\\temp\\ERR091572.sorted.rg.md.bqsr.bam.hcidx"));

        List<String> infos = new ArrayList<>(560);
        String line;
        while ((line = bufferedReader.readLine())!=null){
            infos.add(line);
        }

        VariantCaller.setPreprocessInfo(infos);
        System.out.println();


    }




}
