package org.scut.ccnl.genomics;

import htsjdk.samtools.util.PeekableIterator;
import org.broadinstitute.gatk.engine.traversals.TraverseActiveRegions;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;



public class GATKRecordSubListIterator implements Iterator<SubIteratorItem> {
    private PeekableIterator<GATKSAMRecord> peekableIterator;
    private GATKSAMRecord nextObject;


    private final GenomeLocParser parser;


    /** Constructs a new iterator that wraps the supplied iterator. */
    public GATKRecordSubListIterator(Iterator<GATKSAMRecord> iterator,
                                     GenomeLocParser parser
                                     ) {
        this.peekableIterator = new PeekableIterator<>(iterator);
        this.parser = parser;
    }

    public GATKRecordSubListIterator(PeekableLastIterator<GATKSAMRecord> iterator,
                                     GenomeLocParser parser) {
        this.peekableIterator = iterator;
        this.parser = parser;
    }


    /** True if there are more items, in which case both next() and peek() will return a value. */
    public boolean hasNext() {
        return peekableIterator.hasNext();
    }

    @Override
    public SubIteratorItem next() {
        return new SubIteratorItem(peekableIterator, parser);
    }

    /** Unsupported Operation. */
    public void remove() {
        throw new UnsupportedOperationException("Not supported: remove");
    }


}
