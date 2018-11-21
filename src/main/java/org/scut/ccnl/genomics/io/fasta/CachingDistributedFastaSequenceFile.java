package org.scut.ccnl.genomics.io.fasta;

import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.util.StringUtil;
import org.apache.log4j.Priority;
import org.broadinstitute.gatk.utils.BaseUtils;
import org.broadinstitute.gatk.utils.exceptions.ReviewedGATKException;
import org.scut.ccnl.genomics.GlobalArgument;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by shedfree on 2017/2/27.
 */
public class CachingDistributedFastaSequenceFile extends DistributedFastaSequenceFile implements AutoCloseable {
    protected static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(CachingDistributedFastaSequenceFile.class);

    private final DistributedFastaSequenceFile distributedFastaSequenceFile;

    /** do we want to print debugging information about cache efficiency? */
    private static final boolean PRINT_EFFICIENCY = false;

    /** If we are printing efficiency info, what frequency should we do it at? */
    private static final int PRINT_FREQUENCY = 10000;

    /** The default cache size in bp */
    public static final long DEFAULT_CACHE_SIZE = 1000000;

    /** The cache size of this CachingIndexedFastaSequenceFile */
    private final long cacheSize;

    /** When we have a cache miss at position X, we load sequence from X - cacheMissBackup */
    private final long cacheMissBackup;

    /**
     * If true, we will preserve the case of the original base in the genome
     */
    private final boolean preserveCase;

    /**
     * If true, we will preserve the IUPAC bases in the genome
     */
    private final boolean preserveIUPAC;

    // information about checking efficiency
    long cacheHits = 0;
    long cacheMisses = 0;



    /**
     * Thread local cache to allow multi-threaded use of this class
     */
    private ThreadLocal<CachingDistributedFastaSequenceFile.Cache> cache;
    {
        cache = new ThreadLocal<CachingDistributedFastaSequenceFile.Cache> () {
            @Override protected CachingDistributedFastaSequenceFile.Cache initialValue() {
                return new CachingDistributedFastaSequenceFile.Cache();
            }
        };
    }


    /**
     * Open the given indexed fasta sequence file.  Throw an exception if the file cannot be opened.
     *
     * Looks for a index file for fasta on disk
     * Uses provided cacheSize instead of the default
     *
     * @param fasta The file to open.
     * @param cacheSize the size of the cache to use in this CachingIndexedFastaReader, must be >= 0
     * @param preserveCase If true, we will keep the case of the underlying bases in the FASTA, otherwise everything is converted to upper case
     * @param preserveIUPAC If true, we will keep the IUPAC bases in the FASTA, otherwise they are converted to Ns
     */
    public CachingDistributedFastaSequenceFile(final DistributedFastaSequenceFile fasta, final long cacheSize, final boolean preserveCase, final boolean preserveIUPAC) throws FileNotFoundException {
        distributedFastaSequenceFile = fasta;
        if ( cacheSize < 0 ) throw new IllegalArgumentException("cacheSize must be > 0");
        this.cacheSize = cacheSize;
        this.cacheMissBackup = Math.max(cacheSize / 1000, 1);
        this.preserveCase = preserveCase;
        this.preserveIUPAC = preserveIUPAC;
    }


    /**
     * Open the given indexed fasta sequence file.  Throw an exception if the file cannot be opened.
     *
     * Looks for a index file for fasta on disk.
     * This CachingIndexedFastaReader will convert all FASTA bases to upper cases under the hood
     *
     * @param fasta The file to open.
     */
    public CachingDistributedFastaSequenceFile(final DistributedFastaSequenceFile fasta) throws FileNotFoundException {
        this(fasta, false);
    }

    /**
     * Open the given indexed fasta sequence file.  Throw an exception if the file cannot be opened.
     *
     * Looks for a index file for fasta on disk
     *
     * @param fasta The file to open.
     * @param preserveCase If true, we will keep the case of the underlying bases in the FASTA, otherwise everything is converted to upper case
     */
    public CachingDistributedFastaSequenceFile(final DistributedFastaSequenceFile fasta, final boolean preserveCase) throws FileNotFoundException {
        this(fasta, DEFAULT_CACHE_SIZE, preserveCase, false);
    }

    /**
     * Open the given indexed fasta sequence file.  Throw an exception if the file cannot be opened.
     *
     * Looks for a index file for fasta on disk
     *
     * @param fasta The file to open.
     * @param preserveCase If true, we will keep the case of the underlying bases in the FASTA, otherwise everything is converted to upper case
     * @param preserveIUPAC If true, we will keep the IUPAC bases in the FASTA, otherwise they are converted to Ns
     */
    public CachingDistributedFastaSequenceFile(final DistributedFastaSequenceFile fasta, final boolean preserveCase, final boolean preserveIUPAC) throws FileNotFoundException {
        this(fasta, DEFAULT_CACHE_SIZE, preserveCase, preserveIUPAC);
    }

    /**
     * Create reference data source from fasta file, after performing several preliminary checks on the file.
     * This static utility was refactored from the constructor of ReferenceDataSource.
     * Possibly may be better as an overloaded constructor.
     * @param fastaFile Fasta file to be used as reference
     * @return A new instance of a CachingIndexedFastaSequenceFile.
     */
    public static DistributedFastaSequenceFile checkAndCreate(final DistributedFastaSequenceFile fastaFile) {
        // check

        // Read reference data by creating an IndexedFastaSequenceFile.
        try {
            return new CachingDistributedFastaSequenceFile(fastaFile);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Open the given indexed fasta sequence file.  Throw an exception if the file cannot be opened.
     *
     * Looks for a index file for fasta on disk
     * Uses provided cacheSize instead of the default
     *
     * @param fasta The file to open.
     * @param cacheSize the size of the cache to use in this CachingIndexedFastaReader, must be >= 0
     */
    public CachingDistributedFastaSequenceFile(final DistributedFastaSequenceFile fasta, final long cacheSize ) throws FileNotFoundException {
        this(fasta, cacheSize, false, false);
    }

    /**
     * Print the efficiency (hits / queries) to logger with priority
     */
    public void printEfficiency(final Priority priority) {
        logger.log(priority, String.format("### CachingIndexedFastaReader: hits=%d misses=%d efficiency %.6f%%", cacheHits, cacheMisses, calcEfficiency()));
    }

    /**
     * Returns the efficiency (% of hits of all queries) of this object
     * @return
     */
    public double calcEfficiency() {
        return 100.0 * cacheHits / (cacheMisses + cacheHits * 1.0);
    }

    /**
     * @return the number of cache hits that have occurred
     */
    public long getCacheHits() {
        return cacheHits;
    }

    /**
     * @return the number of cache misses that have occurred
     */
    public long getCacheMisses() {
        return cacheMisses;
    }

    /**
     * @return the size of the cache we are using
     */
    public long getCacheSize() {
        return cacheSize;
    }

    /**
     * Is this CachingIndexedFastaReader keeping the original case of bases in the fasta, or is
     * everything being made upper case?
     *
     * @return true if the bases coming from this reader are in the original case in the fasta, false if they are all upper cased
     */
    public boolean isPreservingCase() {
        return preserveCase;
    }

    /**
     * Is uppercasing bases?
     *
     * @return true if bases coming from this CachingIndexedFastaSequenceFile are all upper cased, false if this reader are in the original case in the fasta
     */
    public boolean isUppercasingBases() {
        return ! isPreservingCase();
    }

    /**
     * Is this CachingIndexedFastaReader keeping the IUPAC bases in the fasta, or is it turning them into Ns?
     *
     * @return true if the IUPAC bases coming from this reader are not modified
     */
    public boolean isPreservingIUPAC() {
        return preserveIUPAC;
    }

    /**
     * Gets the subsequence of the contig in the range [start,stop]
     *
     * Uses the sequence cache if possible, or updates the cache to handle the request.  If the range
     * is larger than the cache itself, just loads the sequence directly, not changing the cache at all
     *
     * @param contig Contig whose subsequence to retrieve.
     * @param start inclusive, 1-based start of region.
     * @param stop inclusive, 1-based stop of region.
     * @return The partial reference sequence associated with this range.  If preserveCase is false, then
     *         all of the bases in the ReferenceSequence returned by this method will be upper cased.
     */
    @Override
    public ReferenceSequence getSubsequenceAt( final String contig, long start, final long stop ) {
        final ReferenceSequence result;
        final CachingDistributedFastaSequenceFile.Cache myCache = cache.get();

        if ( (stop - start) >= cacheSize ) {
            cacheMisses++;
            result = distributedFastaSequenceFile.getSubsequenceAt(contig, start, stop);
            if ( ! preserveCase ) StringUtil.toUpperCase(result.getBases());
            if ( ! preserveIUPAC ) BaseUtils.convertIUPACtoN(result.getBases(), true, start < 1);
        } else {
            // todo -- potential optimization is to check if contig.name == contig, as this in general will be true
            SAMSequenceRecord contigInfo = distributedFastaSequenceFile.getSequenceDictionary().getSequence(contig);

            if (stop > contigInfo.getSequenceLength())
                throw new SAMException("Query asks for data past end of contig");

            if ( start < myCache.start || stop > myCache.stop || myCache.seq == null || myCache.seq.getContigIndex() != contigInfo.getSequenceIndex() ) {
                cacheMisses++;
                myCache.start = Math.max(start - cacheMissBackup, 0);
                myCache.stop  = Math.min(start + cacheSize + cacheMissBackup, contigInfo.getSequenceLength());
                myCache.seq   = distributedFastaSequenceFile.getSubsequenceAt(contig, myCache.start, myCache.stop);

                // convert all of the bases in the sequence to upper case if we aren't preserving cases
                if ( ! preserveCase ) StringUtil.toUpperCase(myCache.seq.getBases());
                if ( ! preserveIUPAC ) BaseUtils.convertIUPACtoN(myCache.seq.getBases(), true, myCache.start == 0);
            } else {
                cacheHits++;
            }

            // at this point we determine where in the cache we want to extract the requested subsequence
            final int cacheOffsetStart = (int)(start - myCache.start);
            final int cacheOffsetStop = (int)(stop - start + cacheOffsetStart + 1);

            try {
                result = new ReferenceSequence(myCache.seq.getName(), myCache.seq.getContigIndex(), Arrays.copyOfRange(myCache.seq.getBases(), cacheOffsetStart, cacheOffsetStop));
            } catch ( ArrayIndexOutOfBoundsException e ) {
                throw new ReviewedGATKException(String.format("BUG: bad array indexing.  Cache start %d and end %d, request start %d end %d, offset start %d and end %d, base size %d",
                        myCache.start, myCache.stop, start, stop, cacheOffsetStart, cacheOffsetStop, myCache.seq.getBases().length), e);
            }
        }

        // for debugging -- print out our efficiency if requested
        if ( PRINT_EFFICIENCY && (getCacheHits() + getCacheMisses()) % PRINT_FREQUENCY == 0 )
            printEfficiency(Priority.INFO);

        return result;
    }

    @Override
    public SAMSequenceDictionary getSequenceDictionary() {
        return  distributedFastaSequenceFile.getSequenceDictionary();
    }

    @Override
    public ReferenceSequence getSequence(String contig) {
        return distributedFastaSequenceFile.getSequence(contig);
    }

    @Override
    public void close(){
        try {
            distributedFastaSequenceFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Represents a specific cached sequence, with a specific start and stop, as well as the bases */
    protected static class Cache {
        long start = -1, stop = -1;
        ReferenceSequence seq = null;
    }

    public static CachingDistributedFastaSequenceFile getFastaSeqFile(GlobalArgument argument)throws Exception{
        switch (argument.REFERENCE_DB){
            case "nfs":
                return new CachingDistributedFastaSequenceFile(new NFSFasta(argument));
            default:
                logger.error("unknown ref db type");
                return null;
        }
    }

}
