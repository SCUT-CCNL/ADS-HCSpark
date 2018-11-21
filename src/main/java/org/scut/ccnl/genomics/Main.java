package org.scut.ccnl.genomics;

public class Main {

    static {
        try {
            Class.forName("org.broadinstitute.gatk.tools.walkers.haplotypecaller.HaplotypeCaller");
            Class.forName("org.broadinstitute.gatk.utils.MRUCachingSAMSequenceDictionary");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
