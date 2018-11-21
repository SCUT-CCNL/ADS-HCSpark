package org.scut.ccnl.genomics;

import htsjdk.samtools.SAMRecord;
import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.iterators.GATKSAMIterator;
import org.broadinstitute.gatk.utils.sam.AlignmentUtils;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;

import java.util.Iterator;

/**
 * Created by shedfree on 2017/5/18.
 */
public class ReadFormattingIterator implements Iterator<GATKSAMRecord> {
    /**
     * Logger.
     */
    final protected static Logger logger = Logger.getLogger(org.broadinstitute.gatk.engine.iterators.ReadFormattingIterator.class);

    /**
     * Iterator to which to pass
     */
    private Iterator<GATKSAMRecord> wrappedIterator;

    /**
     * True if original base qualities should be used.
     */
    private final boolean useOriginalBaseQualities;

    /**
     * Positive if there is a default Base Quality value to fill in the reads with.
     */
    private final byte defaultBaseQualities;


    /**
     * Decorate the given iterator inside a ReadWrappingIterator.
     * @param wrappedIterator iterator
     * @param useOriginalBaseQualities true if original base qualities should be used
     * @param defaultBaseQualities if the reads have incomplete quality scores, set them all to defaultBaseQuality.
     */
    public ReadFormattingIterator(Iterator<GATKSAMRecord> wrappedIterator, boolean useOriginalBaseQualities, byte defaultBaseQualities) {
        this.wrappedIterator = wrappedIterator;
        this.useOriginalBaseQualities = useOriginalBaseQualities;
        this.defaultBaseQualities = defaultBaseQualities;

    }

    /**
     * Convenience function for use in foreach loops.  Dangerous because it does not actually
     * reset the iterator.
     * @return An iterator through the current data stream.
     */
//    public Iterable<GATKSAMRecord> iterator() {
//        // NOTE: this iterator doesn't perform any kind of reset operation; it just returns itself.
//        //       can we do something better?  Do we really have to provide support for the Iterable interface?
//        return this;
//    }

    /**
     * Close this iterator.
     */
//    public void close() {
//        wrappedIterator.close();
//    }

    /**
     * Does the iterator contain more values?
     * @return True if there are more left to return, false otherwise.
     */
    public boolean hasNext() {

        return wrappedIterator.hasNext();
    }

    /**
     * Get the next value in the sequence.
     * @return Next value in the sequence.  By convention, a NoSuchElementException should be thrown if
     *         no next exists.
     */
    public GATKSAMRecord next() {

        GATKSAMRecord rec = wrappedIterator.next();

        // Always consolidate the cigar string into canonical form, collapsing zero-length / repeated cigar elements.
        // Downstream code (like LocusIteratorByState) cannot necessarily handle non-consolidated cigar strings.
        rec.setCigar(AlignmentUtils.consolidateCigar(rec.getCigar()));

        // if we are using default quals, check if we need them, and add if necessary.
        // 1. we need if reads are lacking or have incomplete quality scores
        // 2. we add if defaultBaseQualities has a positive value
        if (defaultBaseQualities >= 0) {
            byte reads [] = rec.getReadBases();
            byte quals [] = rec.getBaseQualities();
            if (quals == null || quals.length < reads.length) {
                byte new_quals [] = new byte [reads.length];
                for (int i=0; i<reads.length; i++)
                    new_quals[i] = defaultBaseQualities;
                rec.setBaseQualities(new_quals);
            }
        }

        // if we are using original quals, set them now if they are present in the record
        if ( useOriginalBaseQualities ) {
            byte[] originalQuals = rec.getOriginalBaseQualities();
            if ( originalQuals != null )
                rec.setBaseQualities(originalQuals);
        }

        return rec;
    }

    /**
     * Remove the current element from the list.  Unsupported in this wrapper.
     */
    public void remove() { throw new UnsupportedOperationException("Cannot remove from a ReadWrappingIterator"); }
}
