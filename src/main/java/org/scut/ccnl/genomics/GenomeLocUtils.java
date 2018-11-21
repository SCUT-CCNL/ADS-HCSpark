package org.scut.ccnl.genomics;


import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;

public class GenomeLocUtils {
    public static boolean overlapsP(GenomeLoc left, GATKSAMRecord right, GenomeLocParser parser) {
        return ! disjointP(left, right ,parser);
    }

    public static boolean disjointP(GenomeLoc left, GATKSAMRecord right, GenomeLocParser parser) {
        return left.getContigIndex() != parser.getContigIndex(right.getContig()) || left.getStart() > right.getEnd() || right.getStart() > left.getStop();
    }

}
