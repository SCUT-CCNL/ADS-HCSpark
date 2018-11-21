package org.scut.ccnl.genomics;

import htsjdk.samtools.util.PeekableIterator;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class SubIteratorItem implements Iterator<GATKSAMRecord> {

    private PeekableIterator<GATKSAMRecord> shard;
    private GenomeLocParser parser;
    private String contig;
    private int start;

    public SubIteratorItem(PeekableIterator<GATKSAMRecord> shard, GenomeLocParser parser) {
        this.shard = shard;
        this.parser = parser;
        this.contig = shard.peek().getContig();
        this.start = shard.peek().getStart();
    }

    public boolean hasNextShard(){
        return shard.hasNext();
    }

    @Override
    public boolean hasNext() {
        if (shard.hasNext()){
            if(shard.peek().getContig().equals(contig)){
                return true;
            }else{
                return false;
            }
        }
        return false;
    }

    @Override
    public GATKSAMRecord next() {
        return shard.next();
    }

    public Iterator<GATKSAMRecord> getIterator(){
        return shard;
    }

    public String getContig() {
        return contig;
    }

    public int getStart() {
        return start > 300 ? start - 300: 1;
    }

    public int getOriginalStart(){
        return start;
    }

    public int getStop() {
        return parser.getContigInfo(contig).getSequenceLength();
    }
}
