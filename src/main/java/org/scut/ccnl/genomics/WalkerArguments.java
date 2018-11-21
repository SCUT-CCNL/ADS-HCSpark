package org.scut.ccnl.genomics;


import org.scut.ccnl.genomics.io.fasta.CachingDistributedFastaSequenceFile;

import java.util.Set;

public class WalkerArguments {

    private CachingDistributedFastaSequenceFile fastaSequenceFile;
    private GlobalArgument globalArgument;

    public WalkerArguments(GlobalArgument globalArgument,
            CachingDistributedFastaSequenceFile fastaSequenceFile){
        this.globalArgument = globalArgument;
        this.fastaSequenceFile = fastaSequenceFile;
    }

    public CachingDistributedFastaSequenceFile getFastaSequenceFile() {
        return fastaSequenceFile;
    }

    public Set<String> getSampleList(){
        return globalArgument.sampleList;
    }
}
