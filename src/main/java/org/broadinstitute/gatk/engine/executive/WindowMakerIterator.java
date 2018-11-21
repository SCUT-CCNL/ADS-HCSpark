/*
* Copyright 2012-2016 Broad Institute, Inc.
*
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
* THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package org.broadinstitute.gatk.engine.executive;

import htsjdk.samtools.util.PeekableIterator;
import org.broadinstitute.gatk.engine.ReadProperties;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.contexts.AlignmentContext;
import org.broadinstitute.gatk.utils.downsampling.DownsampleType;
import org.broadinstitute.gatk.utils.downsampling.DownsamplingMethod;
import org.broadinstitute.gatk.utils.locusiterator.LocusIterator;
import org.broadinstitute.gatk.utils.locusiterator.LocusIteratorByState;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;

import java.util.*;

/**
 * Transforms an iterator of reads which overlap the given interval list into an iterator of covered single-base loci
 * completely contained within the interval list.  To do this, it creates a LocusIteratorByState which will emit a single-bp
 * locus for every base covered by the read iterator, then uses the WindowMakerIterator.advance() to filter down that stream of
 * loci to only those covered by the given interval list.
 *
 * Example:
 * Incoming stream of reads: A:chr20:1-5, B:chr20:2-6, C:chr20:2-7, D:chr20:3-8, E:chr20:5-10
 * Incoming intervals: chr20:3-7
 *
 * Locus iterator by state will produce the following stream of data:
 *  chr1:1 {A}, chr1:2 {A,B,C}, chr1:3 {A,B,C,D}, chr1:4 {A,B,C,D}, chr1:5 {A,B,C,D,E},
 *  chr1:6 {B,C,D,E}, chr1:7 {C,D,E}, chr1:8 {D,E}, chr1:9 {E}, chr1:10 {E}
 *
 * WindowMakerIterator will then filter the incoming stream, emitting the following stream:
 *  chr1:3 {A,B,C,D}, chr1:4 {A,B,C,D}, chr1:5 {A,B,C,D,E}, chr1:6 {B,C,D,E}, chr1:7 {C,D,E}
 *
 * @author mhanna
 * @version 0.1
 */
public class WindowMakerIterator extends LocusIterator {
    /**
     * Source information for iteration.
     */
    private final ReadProperties sourceInfo;

    /**
     * Hold the read iterator so that it can be closed later.
     */
    private Iterator<GATKSAMRecord> readIterator;
    //private final GATKSAMRecordIterator readIterator;

    /**
     * The data source for reads.  Will probably come directly from the BAM file.
     */
    private PeekableIterator<AlignmentContext> sourceIterator;

    /**
     * In the case of monolithic sharding, this case returns whether the only shard has been generated.
     */
    private boolean shardGenerated = false;

    /**
     * The alignment context to return from this shard's iterator.  Lazy implementation: the iterator will not find the
     * currentAlignmentContext until absolutely required to do so.   If currentAlignmentContext is null and advance()
     * doesn't populate it, no more elements are available.  If currentAlignmentContext is non-null, currentAlignmentContext
     * should be returned by next().
     */
    private AlignmentContext currentAlignmentContext;

    /**
     * Create a new window maker with the given iterator as a data source, covering
     * the given intervals.
     * @param iterator The data source for this window.
     * @param intervals The set of intervals over which to traverse.
     * @param sampleNames The complete set of sample names in the reads in shard
     */

    private LocusIteratorByState libs;


    public WindowMakerIterator(Iterator<GATKSAMRecord> readIterator,
                               GenomeLocParser genomeLocParser,GenomeLoc locus,
                               Collection<String> samplenames){
        this.sourceInfo = null;
        this.readIterator = readIterator;
        this.libs = new LocusIteratorByState(this.readIterator,
                new DownsamplingMethod(DownsampleType.BY_SAMPLE,500,null),true,
                true,genomeLocParser, samplenames);
        this.sourceIterator = new PeekableIterator<AlignmentContext>(libs);
        this.locus = locus;

    }


    public void remove() {
        throw new UnsupportedOperationException("Cannot remove from a window maker.");
    }

    public void close() {
        this.readIterator = null;
        this.libs = null;
        this.sourceIterator = null;
        this.locus = null;
    }

    public boolean hasNext() {
        advance();
        return currentAlignmentContext != null;
    }

    public AlignmentContext next() {
        if(!hasNext()) throw new NoSuchElementException("WindowMakerIterator is out of elements for this interval.");

        // Consume this alignment context.
        AlignmentContext toReturn = currentAlignmentContext;
        currentAlignmentContext = null;

        // Return the current element.
        return toReturn;
    }

    public ReadProperties getSourceInfo() {
        return sourceInfo;
    }

    public WindowMakerIterator iterator() {
        return this;
    }

    @Override
    public LocusIteratorByState getLIBS() {
        return libs;
    }

    /**
     * The locus for which this iterator is currently returning reads.
     * 暂时不用interval，下面这个设置为null
     */
    private GenomeLoc locus;

    public GenomeLoc getLocus() {
        return locus;
    }

    private void advance() {
        // Need to find the next element that is not past shard boundaries.  If we travel past the edge of
        // shard boundaries, stop and let the next interval pick it up.
        while(currentAlignmentContext == null && sourceIterator.hasNext()) {
            // Advance the iterator and try again.
            AlignmentContext candidateAlignmentContext = sourceIterator.peek();
            // TODO: 2017/5/31
            // 原本这里弄成这样，是因为原本是顺序读取的，判断candidateAlignmentContext是否在限制的interval范围内，如果在范围的右边则停止读取
            // 即将修改的不用这样判断，因为要读取的都是要处理的
            currentAlignmentContext = sourceIterator.next();
//            if(locus == null) {
//                // No filter present.  Return everything that LocusIteratorByState provides us.
//                currentAlignmentContext = sourceIterator.next();
//            }
//            else if(locus.isPast(candidateAlignmentContext.getLocation()))
//                // Found a locus before the current window; claim this alignment context and throw it away.
//                sourceIterator.next();
//            else if(locus.containsP(candidateAlignmentContext.getLocation())) {
//                // Found a locus within the current window; claim this alignment context and call it the next entry.
//                currentAlignmentContext = sourceIterator.next();
//            }
//            else if(locus.isBefore(candidateAlignmentContext.getLocation())) {
//                // Whoops.  Skipped passed the end of the region.  Iteration for this window is complete.  Do
//                // not claim this alignment context in case it is part of the next shard.
//                break;
//            }
//            else
//                throw new ReviewedGATKException("BUG: filtering locus does not contain, is not before, and is not past the given alignment context");
        }
    }

}
