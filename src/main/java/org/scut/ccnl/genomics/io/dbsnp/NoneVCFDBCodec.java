package org.scut.ccnl.genomics.io.dbsnp;

import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;

import java.util.List;


public class NoneVCFDBCodec extends VCFDBCodec {
    @Override
    public void close() {

    }

    @Override
    public RefMetaDataTracker parserLocation(GenomeLoc loc, GenomeLocParser parser) {
        return new RefMetaDataTracker();
    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser) {
        return null;
    }
}
