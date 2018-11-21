package org.scut.ccnl.genomics.io.fasta;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;
import htsjdk.samtools.reference.ReferenceSequence;
import org.broadinstitute.gatk.utils.fasta.CachingIndexedFastaSequenceFile;
import org.scut.ccnl.genomics.GlobalArgument;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.broadinstitute.gatk.utils.fasta.CachingIndexedFastaSequenceFile.DEFAULT_CACHE_SIZE;


public class NFSFasta extends DistributedFastaSequenceFile implements Closeable {


    private IndexedFastaSequenceFile indexedFastaSequenceFile;
    private FastaSequenceIndex index = null;

    public NFSFasta(GlobalArgument args){
        try {
            indexedFastaSequenceFile = new CachingIndexedFastaSequenceFile(new File(args.FASTA),args.fastaIndex, args.sequenceDictionary,DEFAULT_CACHE_SIZE);
            sequenceDictionary = args.sequenceDictionary;
            index = args.fastaIndex;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        indexedFastaSequenceFile.close();
    }

    @Override
    public ReferenceSequence getSubsequenceAt(String contig, long start, long stop) {
        return indexedFastaSequenceFile.getSubsequenceAt(contig,start,stop);
    }

    @Override
    public SAMSequenceDictionary getSequenceDictionary() {
        return sequenceDictionary;
    }

    @Override
    public ReferenceSequence getSequence(String contig) {
        return getSubsequenceAt( contig, 1, (int)index.getIndexEntry(contig).getSize() );
    }
}
