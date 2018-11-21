package org.scut.ccnl.genomics.io.fasta;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.util.BufferedLineReader;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class DistributedFastaSequenceFile implements Closeable {

    protected static final int DEFAULT_LINE_BASES = 50;

    protected SAMSequenceDictionary sequenceDictionary;

    public DistributedFastaSequenceFile(){}

    public DistributedFastaSequenceFile(SAMSequenceDictionary sequenceDictionary){
        this.sequenceDictionary = sequenceDictionary;
    }

    public void setSequenceDictionary(SAMSequenceDictionary sequenceDictionary){
        this.sequenceDictionary = sequenceDictionary;
    }

    public abstract ReferenceSequence getSubsequenceAt(final String contig, long start, final long stop ) ;

    /**
     * Retrieves the sequence dictionary for the fasta file.
     * @return sequence dictionary of the fasta.
     */
    public abstract SAMSequenceDictionary getSequenceDictionary();
//    {
//        return sequenceDictionary;
//    }


    public abstract ReferenceSequence getSequence( String contig );

    public static SAMSequenceDictionary getSequenceDictionary(String dictname){
        final Path dictionary = new File(dictname).toPath();
        SAMSequenceDictionary sequenceDictionary = null;
        try {
            final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
            final BufferedLineReader reader = new BufferedLineReader(Files.newInputStream(dictionary));
            final SAMFileHeader header = codec.decode(reader,
                    dictionary.toString());
            if (header.getSequenceDictionary() != null && !header.getSequenceDictionary().isEmpty()) {
                sequenceDictionary = header.getSequenceDictionary();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //FastaSequenceIndex index = new FastaSequenceIndex(new File("D:\\genomic\\hg19.fasta.fai"));

        return sequenceDictionary;
    }

}
