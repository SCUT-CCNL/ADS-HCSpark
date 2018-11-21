package org.scut.ccnl.genomics.test;

import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.scut.ccnl.genomics.Arguments;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.io.dbsnp.ImpalaVCFCodec;
import org.scut.ccnl.genomics.io.dbsnp.MysqlVCFCodec;
import org.scut.ccnl.genomics.io.fasta.DistributedFastaSequenceFile;

/**
 * Created by shedfree on 2017/12/19.
 */
public class LinuxTest {
    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(1000);
        GenomeLocParser parser = new GenomeLocParser(
                DistributedFastaSequenceFile.getSequenceDictionary("/data/home/wzz/temp/hg19.dict"));
        parser.setMRUCachingSAMSequenceDictionary();
        Arguments.init();
        System.setProperty(Constants.MYSQL_URL, Arguments.MYSQL_URL);
        System.setProperty(Constants.MYSQL_USERNAME, Arguments.MYSQL_USERNAME);
        System.setProperty(Constants.MYSQL_PASSWORD, Arguments.MYSQL_PASSWORD);
        System.setProperty(Constants.IMPALA_URL, ImpalaVCFCodec.CONNECTION_URL);
        ImpalaVCFCodec impalaVCFCodec = new ImpalaVCFCodec("dbsnp_kudu");
        MysqlVCFCodec mysqlVCFCodec = new MysqlVCFCodec();


        RefMetaDataTracker a = impalaVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
        RefMetaDataTracker b = mysqlVCFCodec.parserLocation(parser.createGenomeLoc("chr1",209280662,209290656),parser);
        System.out.println(a.getBoundRodTracks().size());
    }
}
