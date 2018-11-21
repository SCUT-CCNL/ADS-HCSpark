package org.scut.ccnl.genomics.io.dbsnp;


import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.hbase.async.KeyValue;
import org.scut.ccnl.genomics.io.HbaseDAO;

import java.util.ArrayList;
import java.util.List;


public class HbaseVCFCodec extends VCFDBCodec {

    public HbaseVCFCodec(){
    }

    @Override
    public void close()  {
    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser){
        final String name = "dbsnp";


        List<GATKFeature> features = new ArrayList<>();
        List<ArrayList<KeyValue>> results = null;
        try {
            // TODO: getResultScan这个API需要修改
            results = HbaseDAO.getResultScan(
                        HbaseDAO.getDbsnpRowKey(parser, loc.getContig(), loc.getStart()),
                        HbaseDAO.getDbsnpRowKey(parser, loc.getContig(), loc.getStop()), "base");

            for(ArrayList<KeyValue> r : results) {
                features.add(new GATKFeature.TribbleGATKFeature(parser,parseVCFfromHBase(r),name));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }
        return features;
    }

    public static void main(String[] args) {
//        GenomeLocParser parser = new GenomeLocParser(getSequenceDictionary(Arguments.FASTA_DICT));
//        new HbaseVCFCodec().parserLocation(parser.createGenomeLoc("chr1",10100,11000),parser);
    }
}
