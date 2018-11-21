package org.scut.ccnl.genomics.io;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class PriorityFileSplit extends FileSplit implements Comparable<PriorityFileSplit> {
    int prority;
    public PriorityFileSplit(Path file, long start, long length, String[] hosts, int prority){
        super(file,start,length,hosts);
        this.prority = prority;
    }

    // 默认这样的是不需要分块的，优先级降为最低
    public PriorityFileSplit(FileSplit fileSplit) throws IOException{
        super(fileSplit.getPath(),fileSplit.getStart(),fileSplit.getLength(),fileSplit.getLocations());
        this.prority = 0;
    }

    public PriorityFileSplit(FileSplit fileSplit, int prority) throws IOException{
        super(fileSplit.getPath(),fileSplit.getStart(),fileSplit.getLength(),fileSplit.getLocations());
        this.prority = prority;
    }

    public int getPrority(){
        return prority;
    }


    @Override
    public int compareTo(PriorityFileSplit o) {
        // 默认逆序（从大到小）排序
        return o.prority-prority;
    }
}
