package org.scut.ccnl.genomics;


import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.FastaSequenceIndex;
import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.fasta.CachingIndexedFastaSequenceFile;
import org.scut.ccnl.genomics.io.fasta.CachingDistributedFastaSequenceFile;
import org.scut.ccnl.genomics.io.fasta.DistributedFastaSequenceFile;

import java.io.File;
import java.io.Serializable;
import java.util.Set;
/*
    能够序列化的参数对象，用于广播
 */
public class GlobalArgument implements Serializable {
    final protected static Logger logger = Logger.getLogger(GlobalArgument.class);
    final public Boolean OBJECT_POOL;
    public boolean OVERLAP = true;

    public String FASTA_INDEX;
    public String FASTA_DICT;
    public String FASTA;

    public FastaSequenceIndex fastaIndex;
    public SAMSequenceDictionary sequenceDictionary;

    public int SHARD_SIZE;
    public int SUBLIST_OVERLAP_SIZE ;
    public int INPUT_FORMAT_OVERLAP_SIZE ;

    public String HBASE_REFERENCE ;
    public String HBASE_DBSNP ;
    public String HBASE_DBSNP_FAMILY_POS;

    // fasta的来源:hbase,mongo,hdfs,nfs
    public String REFERENCE_DB = "nfs";
    // dbsnp的来源:hbase,mysql,none
    public String DBSNP_DB = "none";

    // impala相关配置
    public String IMPALA_DBSNP;

    // 连接Mysql的相关配置
    public String MYSQL_URL = "";
    public String MYSQL_USERNAME="";
    public String MYSQL_PASSWORD="";

    public String DBSNP_PATH = "";
    public boolean DBSNP_CACHE = false;

    // 输出路径
    public String OUTPUT_NAME;
    public boolean sort = false;

//    public GenomeLocParser parser;

    public Set<String> sampleList;

    public void setSampleList(Set<String> sampleList) {
        this.sampleList = sampleList;
    }

    public void setOutput(String OUTPUT_NAME) {
        this.OUTPUT_NAME = OUTPUT_NAME;
    }

    public GlobalArgument(){
        OBJECT_POOL = false;
    }

    // 可能可以避免kryo序列化时的错误
    public GlobalArgument(boolean flag){
        logger.info("init GlobalArgument");

        OBJECT_POOL = Arguments.OBJECT_POOL;
        OVERLAP = Arguments.OVERLAP;

        FASTA_INDEX = Arguments.FASTA_INDEX;
        FASTA_DICT = Arguments.FASTA_DICT;
        FASTA = Arguments.FASTA;

        SHARD_SIZE = Arguments.SHARD_SIZE;
        SUBLIST_OVERLAP_SIZE = Arguments.SUBLIST_OVERLAP_SIZE;
        INPUT_FORMAT_OVERLAP_SIZE = Arguments.INPUT_FORMAT_OVERLAP_SIZE;

        HBASE_REFERENCE = Arguments.HBASE_REFERENCE;
        HBASE_DBSNP_FAMILY_POS = Arguments.HBASE_DBSNP_FAMILY_POS;

        HBASE_DBSNP = Arguments.HBASE_DBSNP;

        REFERENCE_DB = Arguments.REFERENCE_DB;
        DBSNP_DB = Arguments.DBSNP_DB;

        IMPALA_DBSNP = Arguments.IMPALA_DBSNP;

        MYSQL_URL = Arguments.MYSQL_URL;
        MYSQL_USERNAME = Arguments.MYSQL_USERNAME;
        MYSQL_PASSWORD = Arguments.MYSQL_PASSWORD;
        //如果dbsnp-db的类型是Mysql，但是连接的属性不完整则将其转成none
        if(DBSNP_DB.equals("mysql") && (MYSQL_URL.equals("")||MYSQL_USERNAME.equals("")||MYSQL_PASSWORD.equals("")) ){
            logger.error("mysql conf is incomplete " +
                    "[ url:"+MYSQL_URL+" username:"+MYSQL_USERNAME+" password:"+MYSQL_PASSWORD);
            DBSNP_DB = "none";
        }

        DBSNP_PATH = Arguments.DBSNP_PATH;
        DBSNP_CACHE = Arguments.DBSNP_CACHE;

        sequenceDictionary = DistributedFastaSequenceFile.getSequenceDictionary(FASTA_DICT);

        // TODO: 2017/11/10 可能要适配多种来源
        fastaIndex = new FastaSequenceIndex(new File(FASTA_INDEX));

    }

    @Override
    public String toString() {
        return "{" +
                "FASTA='" + FASTA + '\'' +
                ", REFERENCE_DB='" + REFERENCE_DB + '\'' +
                ", DBSNP_DB='" + DBSNP_DB + '\'' +
                '}';
    }
}
