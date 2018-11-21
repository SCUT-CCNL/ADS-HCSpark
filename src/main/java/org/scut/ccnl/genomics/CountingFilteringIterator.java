package org.scut.ccnl.genomics;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.filter.SamRecordFilter;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.CloserUtil;
import org.broadinstitute.gatk.engine.ReadMetrics;
import org.broadinstitute.gatk.engine.filters.ReadFilter;
import org.broadinstitute.gatk.utils.sam.GATKSAMRecord;

import java.util.*;

public class CountingFilteringIterator implements Iterator<GATKSAMRecord> {
    private final Iterator<GATKSAMRecord> iterator;
    private final List<CountingReadFilter> filters = new ArrayList<>();
    private GATKSAMRecord next = null;

    // wrapper around ReadFilters to count the number of filtered reads
    private final class CountingReadFilter extends ReadFilter {
        protected final ReadFilter readFilter;
        protected long counter = 0L;

        public CountingReadFilter(final ReadFilter readFilter) {
            this.readFilter = readFilter;
        }

        @Override
        public boolean filterOut(final SAMRecord record) {
            final boolean result = readFilter.filterOut(record);
            if ( result )
                counter++;
            return result;
        }
    }

    /**
     * Constructor
     *
     * @param iterator  the backing iterator
     * @param filters    the filter (which may be a FilterAggregator)
     */
    public CountingFilteringIterator(Iterator<GATKSAMRecord> iterator, Collection<ReadFilter> filters) {
        this.iterator = iterator;
        for ( final ReadFilter filter : filters )
            this.filters.add(new CountingReadFilter(filter));
        next = getNextRecord();
    }

    /**
     * Returns true if the iteration has more elements.
     *
     * @return  true if the iteration has more elements.  Otherwise returns false.
     */
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return  the next element in the iteration
     * @throws java.util.NoSuchElementException
     */
    public GATKSAMRecord next() {
        if (next == null) {
            throw new NoSuchElementException("Iterator has no more elements.");
        }
        final GATKSAMRecord result = next;
        next = getNextRecord();
        return result;
    }

    /**
     * Required method for Iterator API.
     *
     * @throws UnsupportedOperationException
     */
    public void remove() {
        throw new UnsupportedOperationException("Remove() not supported by CountingFilteringIterator");
    }

    public void close() {
        CloserUtil.close(iterator);

    }

    /**
     * Gets the next record from the underlying iterator that passes the filter
     *
     * @return SAMRecord    the next filter-passing record
     */
    private GATKSAMRecord getNextRecord() {
        while (iterator.hasNext()) {
            GATKSAMRecord record = iterator.next();

            // update only the private copy of the metrics so that we don't need to worry about race conditions
            // that can arise when trying to update the global copy; it was agreed that this is the cleanest solution.

            boolean filtered = false;
            for(SamRecordFilter filter: filters) {
                if(filter.filterOut(record)) {
                    filtered = true;
                    break;
                }
            }
            // TODO wu:对象池的这里可能需要进行回收,也可能不用
            if(!filtered) return record;
        }

        return null;
    }
}

