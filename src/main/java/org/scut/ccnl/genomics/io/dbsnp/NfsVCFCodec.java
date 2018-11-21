package org.scut.ccnl.genomics.io.dbsnp;


import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.CloseableTribbleIterator;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RODRecordListImpl;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.broadinstitute.gatk.utils.refdata.utils.RODRecordList;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.GlobalArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NfsVCFCodec extends VCFDBCodec {

    FeatureReader<VariantContext> reader = null;

    public NfsVCFCodec(GlobalArgument globalArgument) {
        reader =  AbstractFeatureReader.getFeatureReader(globalArgument.DBSNP_PATH,new VCFCodec(),true);
    }

    public NfsVCFCodec(String dbsnpPath) {
        reader =  AbstractFeatureReader.getFeatureReader(dbsnpPath,new VCFCodec(),true);
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser){
        List<GATKFeature> features = new ArrayList<>();
        try {
            CloseableTribbleIterator<VariantContext> iterator = reader.query(loc.getContig(), loc.getStart(), loc.getStop());

            for (VariantContext variantContext : iterator) {
                features.add(new GATKFeature.TribbleGATKFeature(parser, variantContext, Constants.DBSNP_NAME));
            }
            iterator.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return features;
    }
}
