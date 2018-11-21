package org.scut.ccnl.genomics;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.CloserUtil;
import htsjdk.samtools.util.PeekableIterator;

import java.util.Iterator;

public class PeekableLastIterator<Object> extends PeekableIterator<Object> {
    private Object last;

    /** Constructs a new iterator that wraps the supplied iterator. */
    public PeekableLastIterator(Iterator<Object> iterator) {
        super(iterator);
    }

    /** Returns the next object and advances the iterator. */
    @Override
    public Object next() {
        Object retval = super.next();
        rememberLast(retval);
        return retval;
    }

    private void rememberLast(Object retval){
        last = retval;
    }

    public Object getLast(){
        return last;
    }
}
