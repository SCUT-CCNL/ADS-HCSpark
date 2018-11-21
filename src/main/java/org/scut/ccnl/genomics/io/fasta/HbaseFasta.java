package org.scut.ccnl.genomics.io.fasta;

import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.FastaSequenceIndexEntry;
import htsjdk.samtools.reference.ReferenceSequence;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.scut.ccnl.genomics.Arguments;
import org.scut.ccnl.genomics.io.HbaseDAO;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Created by shedfree on 2017/3/24.
 */
public class HbaseFasta extends DistributedFastaSequenceFile implements Closeable {
    public static int HBASE_LINE = 1000*DEFAULT_LINE_BASES;
    public static String TABLE_NAME = "Reference";
    public static String FAMILY_NAME = "attribute";
    public static String COLUMN_NAME = "base";

    public static HbaseFasta hbaseFasta = new HbaseFasta();



    HbaseDAO hbaseDAO = new HbaseDAO();

    //临时
    FastaSequenceIndex index = new FastaSequenceIndex(new File(Arguments.FASTA_INDEX));
    static {
        hbaseFasta.setSequenceDictionary(getSequenceDictionary(Arguments.FASTA_DICT));
    }

    @Override
    public void close() throws IOException {
        hbaseDAO.close();
    }

    @Override
    public ReferenceSequence getSubsequenceAt(String contig, long start, long stop) {
        int length = (int)(stop - start + 1);
        if(length<=0)
            throw new SAMException(String.format("Malformed query; start point %d lies after end point %d",start,stop));
        FastaSequenceIndexEntry indexEntry = index.getIndexEntry(contig);

        if(stop > indexEntry.getSize())
            throw new SAMException("Query asks for data past end of contig");
        //前面的行数
        long startline = ((start-1)/HBASE_LINE);
        int startoffset = (int)(start-1)%HBASE_LINE;
        long stopline = (stop-1)/HBASE_LINE + 1;
        ResultScanner result = null;
        StringBuilder sb = new StringBuilder();
        try {
            result =  hbaseDAO.getResultScan(TABLE_NAME,HbaseDAO.getFastaRowkey(contig,startline),HbaseDAO.getFastaRowkey(contig,stopline),FAMILY_NAME,COLUMN_NAME);

            for(Result r : result){
                for(Cell cell :r.listCells()){
                    sb.append(new String(CellUtil.cloneValue(cell)));
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            result.close();
        }
        return new ReferenceSequence(contig,index.getIndexEntry(contig).getSequenceIndex(),sb.substring(startoffset,startoffset+length).getBytes());
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
