package org.seqdoop.hadoop_bam;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.scut.ccnl.genomics.Arguments;
import org.scut.ccnl.genomics.io.PriorityFileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GATKSAMPriorityOverlapInputFormat extends  GATKSAMOverlapInputFormat{

    public List<InputSplit> getSplits(
            List<InputSplit> splits, Configuration cfg)
            throws IOException
    {
        // Align the splits so that they don't cross blocks.

        // addIndexedSplits() requires the given splits to be sorted by file
        // path, so do so. Although FileInputFormat.getSplits() does, at the time
        // of writing this, generate them in that order, we shouldn't rely on it.
        Collections.sort(splits, new Comparator<InputSplit>() {
            public int compare(InputSplit a, InputSplit b) {
                FileSplit fa = (FileSplit)a, fb = (FileSplit)b;
                return fa.getPath().compareTo(fb.getPath());
            }
        });


        List<PriorityFileSplit> additionSplits = new ArrayList<>();

        for(int ii = 0; ii < splits.size(); ii++){
            if(Arguments.PARTITIONS_TO_CHANGE.contains(ii)){
//                 additionSplits.add(new PriorityFileSplit((FileSplit) splits.get(ii),GATKSAMPriorityInputFormat.VERY_HIGH_LEVEL));
                additionSplits.addAll(getSmallSplits(ii, splits, 8,GATKSAMPriorityInputFormat.VERY_HIGH_LEVEL));
            }else{
                additionSplits.add(new PriorityFileSplit((FileSplit) splits.get(ii),GATKSAMPriorityInputFormat.LOW_LEVEL));
            }
        }

        // 根据优先级进行排序
        Collections.sort(additionSplits);
        // 将原本的清空，添加新的
        splits.clear();
        splits.addAll(additionSplits);



        final List<InputSplit> newSplits =
                new ArrayList<InputSplit>(splits.size());

        for (int i = 0; i < splits.size() -1;) {
            i = addProbabilisticSplits(splits, i, newSplits, cfg);
        }
        return filterByInterval(newSplits, cfg);
    }

    protected List<PriorityFileSplit> getSmallSplits(int i, List<InputSplit> splits, int each_split_size, int priority) throws IOException{
        FileSplit fspl = (FileSplit) splits.get(i);

        long all_len = fspl.getLength();
        long each_block_len = (int)Math.ceil(all_len / (64*1024) / each_split_size);
        each_block_len*=(64*1024);
        long start = fspl.getStart();

        List<PriorityFileSplit> tempSplits = new ArrayList<>(each_split_size);

        for (int j = 0; j < each_split_size - 1; j++) {

            PriorityFileSplit temp = new PriorityFileSplit(fspl.getPath(), start, each_block_len, fspl.getLocations(), priority);
            start += each_block_len;
            tempSplits.add(temp);
        }
        // 最后一个分块的大小可能会有些差异
        PriorityFileSplit temp = new PriorityFileSplit(fspl.getPath(), start, fspl.getStart() + all_len - start, fspl.getLocations(),priority);
        tempSplits.add(temp);

        return tempSplits;
    }

}
