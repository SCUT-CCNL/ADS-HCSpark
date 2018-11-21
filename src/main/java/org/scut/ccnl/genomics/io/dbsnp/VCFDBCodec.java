package org.scut.ccnl.genomics.io.dbsnp;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RODRecordListImpl;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.broadinstitute.gatk.utils.refdata.utils.RODRecordList;
import org.hbase.async.KeyValue;
import org.scut.ccnl.genomics.Constants;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public abstract class VCFDBCodec extends VCFCodec implements Closeable {


    public abstract void close();

    public VariantContext parseVCFfromDB(DBsnp dBsnp){
        VariantContextBuilder builder = new VariantContextBuilder();
        builder.source(Constants.DBSNP_NAME);
        builder.chr(dBsnp.getChrom());
        builder.start(dBsnp.getPos());

        final String ref = dBsnp.getRef();
        builder.stop(dBsnp.getStop());

        final String id = dBsnp.getId();
        if(id.equals(VCFConstants.EMPTY_ID_FIELD)) builder.noID(); else builder.id(id);
        // parseAlleles方法是抽象类的方法，protected域
        builder.alleles(parseAlleles(ref,dBsnp.getAlt(),0));

        return builder.make();
    }


    public VariantContext parseVCFfromHBase(List<Cell> cells){
        //pos列族获取的顺序为alt、ref、chrom、ID、pos
        VariantContextBuilder builder = new VariantContextBuilder();
        builder.source("dbsnp");

        String ref = new String(CellUtil.cloneValue(cells.get(3)));
        String alt = new String(CellUtil.cloneValue(cells.get(0)));
        builder.alleles(parseAlleles(ref,alt,0));

        builder.chr(new String(CellUtil.cloneValue(cells.get(1))));

        final String id = new String(CellUtil.cloneValue(cells.get(4)));
        if(id.equals(VCFConstants.EMPTY_ID_FIELD)) builder.noID(); else builder.id(id);
        long start = Long.valueOf(new String(CellUtil.cloneValue(cells.get(2))));
        builder.start(start);
        builder.stop(start+ref.length()-1);

        return builder.make();
    }

    public VariantContext parseVCFfromHBase(ArrayList<KeyValue> keyValues){
        //pos列族获取的顺序为alt、ref、chrom、ID、pos
        VariantContextBuilder builder = new VariantContextBuilder();
        builder.source("dbsnp");

        String ref = new String(keyValues.get(3).value());
        String alt = new String(keyValues.get(0).value());
        builder.alleles(parseAlleles(ref,alt,0));

        builder.chr(new String(keyValues.get(1).value()));

        final String id = new String(keyValues.get(4).value());
        if(id.equals(VCFConstants.EMPTY_ID_FIELD)) builder.noID(); else builder.id(id);
        long start = Long.valueOf(new String(keyValues.get(2).value()));
        builder.start(start);
        builder.stop(start+ref.length()-1);

        return builder.make();
    }

    public VariantContext parseVCFfromImpala(ImpalaDBsnp dBsnp){
        VariantContextBuilder builder = new VariantContextBuilder();
        builder.source(Constants.DBSNP_NAME);
        builder.chr(dBsnp.getChrom());
        builder.start(dBsnp.getPos());

        final String ref = dBsnp.getRef();
        builder.stop(dBsnp.getPos()+ref.length()-1);

        final String id = dBsnp.getRs_id();
        if(id.equals(VCFConstants.EMPTY_ID_FIELD)) builder.noID(); else builder.id(id);
        // parseAlleles方法是抽象类的方法，protected域
        builder.alleles(parseAlleles(ref,dBsnp.getAlt(),0));

        return builder.make();
    }

    public RefMetaDataTracker parserLocation(GenomeLoc loc, GenomeLocParser parser){
        //多个位点的Feature
        RODRecordList rodRecordList = new RODRecordListImpl(Constants.DBSNP_NAME, getGATKFeatures(loc, parser), loc);

        return new RefMetaDataTracker(Arrays.asList(rodRecordList));
    }

    public abstract List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser);

//    public RefMetaDataTracker parserLocation(GenomeLoc loc){
//
//        GenomeLocParser parser = new GenomeLocParser(SparkHC.getSequenceDictionary(Constants.FASTA_DICT));
//        final String name = "dbsnp";
//
//
//        List<GATKFeature> features = new ArrayList<>();
//
//        List<DBsnp> list = iDBsnpOperator.queryByRange(loc.getContig(),loc.getStart(),loc.getStop());
//        for(DBsnp dbsnp: list){
//            features.add(new GATKFeature.TribbleGATKFeature(parser,parseVCFfromDB(dbsnp),name));
//        }


//        for (int i = loc.getStart(); i <= loc.getStop(); i++) {
//            List<DBsnp> list = iDBsnpOperator.queryLoc(loc.getContig(),i);
//            for(DBsnp dbsnp: list){
//                features.add(new GATKFeature.TribbleGATKFeature(parser,parseVCFfromDB(dbsnp),name));
//            }
//        }
//        if(features.size()>0){
//            System.out.println();
//        }
//        //多个位点的Feature
//        RODRecordList rodRecordList = new RODRecordListImpl("dbsnp", features, loc);
//
//        return new RefMetaDataTracker(Arrays.asList(rodRecordList));
//    }
//
//    public static void main(String[] args) {
//        VCFDBCodec codec = new VCFDBCodec();
//        codec.parserLocation(new GenomeLoc("chr2",2072543,2072543));
//
//    }

}
