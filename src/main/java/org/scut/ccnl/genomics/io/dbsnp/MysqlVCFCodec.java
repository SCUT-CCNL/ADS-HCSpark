package org.scut.ccnl.genomics.io.dbsnp;

import org.apache.ibatis.session.SqlSession;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RODRecordListImpl;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.broadinstitute.gatk.utils.refdata.utils.RODRecordList;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.io.MysqlDAO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MysqlVCFCodec extends VCFDBCodec {




    private SqlSession session;
    private IDBsnpOperator iDBsnpOperator;


    public MysqlVCFCodec(){
        session = MysqlDAO.getSqlSessionFactory().openSession();
        iDBsnpOperator = session.getMapper(IDBsnpOperator.class);

    }

    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser){

        List<GATKFeature> features = new ArrayList<>();

        List<DBsnp> list = iDBsnpOperator.queryByRange(loc.getContig(),loc.getStart(),loc.getStop());
        for(DBsnp dbsnp: list){
            features.add(new GATKFeature.TribbleGATKFeature(parser,parseVCFfromDB(dbsnp), Constants.DBSNP_NAME));
        }

        return features;
    }


    public void close(){
        session.close();
    }

}
