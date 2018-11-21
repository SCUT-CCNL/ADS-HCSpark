package htsjdk.samtools.seekablestream;


import hdfs.jsr203.HadoopFileSystem;
import hdfs.jsr203.HadoopFileSystemProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HdfsStreamFactory {

    static HadoopFileSystemProvider hfsProvider = new HadoopFileSystemProvider();
    static HadoopFileSystem hfs;
    final static Set<StandardOpenOption> option = Collections.singleton(StandardOpenOption.READ);

    static {
        try {
            hfs = new HadoopFileSystem(hfsProvider, "cu09", 8020);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Path getHadoopPath(String path){
        return hfs.getPath(path);
    }

    public static SeekableByteChannel getChannel(Path path){
        try {
            return hfsProvider.newByteChannel(path,option);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static InputStream getInputStream(Path path){
        try {
            return hfsProvider.newInputStream(path,StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static InputStream getInputStream(String path){
        try {
            return hfsProvider.newInputStream(getHadoopPath(path),StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



}
