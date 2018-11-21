package org.scut.ccnl.genomics.io.fasta;

import hdfs.jsr203.HadoopFileSystem;
import hdfs.jsr203.HadoopFileSystemProvider;
import htsjdk.samtools.Defaults;
import htsjdk.samtools.SAMException;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.FastaSequenceIndexEntry;
import htsjdk.samtools.reference.ReferenceSequence;
import org.scut.ccnl.genomics.Arguments;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by shedfree on 2017/4/18.
 */
public class HDFSFasta extends DistributedFastaSequenceFile implements Closeable  {

    public static HDFSFasta hdfsFasta = new HDFSFasta();

    /**
     * The interface facilitating direct access to the fasta.
     */
    private SeekableByteChannel channel;
    //临时
    FastaSequenceIndex index = new FastaSequenceIndex(new File(Arguments.FASTA_INDEX));

    public HDFSFasta(){
        try {
            HadoopFileSystemProvider hfsProvider = new HadoopFileSystemProvider();
            HadoopFileSystem hfs = new HadoopFileSystem(hfsProvider,"mu01",9000);
            Path path = hfs.getPath("user/wzz/hg19.fasta");
            Set<StandardOpenOption> option = new HashSet<>();
            option.add(StandardOpenOption.READ);
            channel = hfsProvider.newByteChannel(path,option);

        }catch(Exception e){
            e.printStackTrace();
        }
    }




    @Override
    public ReferenceSequence getSubsequenceAt(String contig, long start, long stop) {
        if(start > stop + 1)
            throw new SAMException(String.format("Malformed query; start point %d lies after end point %d",start,stop));
        FastaSequenceIndexEntry indexEntry = index.getIndexEntry(contig);

        if(stop > indexEntry.getSize())
            throw new SAMException("Query asks for data past end of contig");

        int length = (int)(stop - start + 1);

        byte[] target = new byte[length];
        ByteBuffer targetBuffer = ByteBuffer.wrap(target);

        final int basesPerLine = indexEntry.getBasesPerLine();
        final int bytesPerLine = indexEntry.getBytesPerLine();
        final int terminatorLength = bytesPerLine - basesPerLine;

        long startOffset = ((start-1)/basesPerLine)*bytesPerLine + (start-1)%basesPerLine;
        // Cast to long so the second argument cannot overflow a signed integer.
        final long minBufferSize = Math.min((long) Defaults.NON_ZERO_BUFFER_SIZE, (long)(length / basesPerLine + 2) * (long)bytesPerLine);
        if (minBufferSize > Integer.MAX_VALUE) throw new SAMException("Buffer is too large: " +  minBufferSize);

        // Allocate a buffer for reading in sequence data.
        final ByteBuffer channelBuffer = ByteBuffer.allocate((int)minBufferSize);

        while(targetBuffer.position() < length) {
            // If the bufferOffset is currently within the eol characters in the string, push the bufferOffset forward to the next printable character.
            startOffset += Math.max((int)(startOffset%bytesPerLine - basesPerLine + 1),0);

            try {
                startOffset += readFromPosition(channel, channelBuffer, indexEntry.getLocation()+startOffset);
            }
            catch(IOException ex) {
                throw new SAMException("Unable to load " + contig + "(" + start + ", " + stop + ") from ", ex);
            }

            // Reset the buffer for outbound transfers.
            channelBuffer.flip();

            // Calculate the size of the next run of bases based on the contents we've already retrieved.
            final int positionInContig = (int)start-1+targetBuffer.position();
            final int nextBaseSpan = Math.min(basesPerLine-positionInContig%basesPerLine,length-targetBuffer.position());
            // Cap the bytes to transfer by limiting the nextBaseSpan to the size of the channel buffer.
            int bytesToTransfer = Math.min(nextBaseSpan,channelBuffer.capacity());

            channelBuffer.limit(channelBuffer.position()+bytesToTransfer);

            while(channelBuffer.hasRemaining()) {
                targetBuffer.put(channelBuffer);

                bytesToTransfer = Math.min(basesPerLine,length-targetBuffer.position());
                channelBuffer.limit(Math.min(channelBuffer.position()+bytesToTransfer+terminatorLength,channelBuffer.capacity()));
                channelBuffer.position(Math.min(channelBuffer.position()+terminatorLength,channelBuffer.capacity()));
            }

            // Reset the buffer for inbound transfers.
            channelBuffer.flip();
        }

        return new ReferenceSequence( contig, indexEntry.getSequenceIndex(), target );
    }

    @Override
    public SAMSequenceDictionary getSequenceDictionary() {
        return sequenceDictionary;
    }



    @Override
    public ReferenceSequence getSequence(String contig) {
        return getSubsequenceAt( contig, 1, (int)index.getIndexEntry(contig).getSize() );
    }

    private static int readFromPosition(final SeekableByteChannel channel, final ByteBuffer buffer, long position) throws IOException {
        if (channel instanceof FileChannel) { // special case to take advantage of native code path
            return ((FileChannel) channel).read(buffer,position);
        } else {
            long oldPos = channel.position();
            try {
                channel.position(position);
                return channel.read(buffer);
            } finally {
                channel.position(oldPos);
            }
        }
    }



    @Override
    public void close() throws IOException {

    }
}
