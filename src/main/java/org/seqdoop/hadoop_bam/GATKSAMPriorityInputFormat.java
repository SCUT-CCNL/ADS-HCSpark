package org.seqdoop.hadoop_bam;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.xpath.Arg;
import org.scut.ccnl.genomics.Arguments;
import org.scut.ccnl.genomics.io.PriorityFileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GATKSAMPriorityInputFormat
        extends GATKSAMInputFormat{
    public final static int LEVEL_SIX = 6;
    public final static int LEVEL_FIVE = 5;
    public final static int LEVEL_FOUR = 4;
    public final static int LEVEL_THREE = 3;
    public final static int LEVEL_TWO = 2;
    public final static int LEVEL_ONE = 1;

    public final static int VERY_HIGH_LEVEL = 3;
    public final static int HIGH_LEVEL = 2;
    public final static int MID_LEVEL = 1;
    public final static int LOW_LEVEL = 0;

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
                additionSplits.addAll(getSmallSplits(ii, splits, Arguments.ADDITION_EACH_SPLIT_SIZE,GATKSAMPriorityInputFormat.VERY_HIGH_LEVEL));
            }else{
                additionSplits.add(new PriorityFileSplit((FileSplit) splits.get(ii),GATKSAMPriorityInputFormat.LOW_LEVEL));
            }
        }


//        int validPartitionNum = Arguments.VALID_PARTITION_NUM;
//
//        if(validPartitionNum ==0){
//            validPartitionNum = splits.size();
//        }
//        double base = validPartitionNum - Math.ceil(0.05 * ( validPartitionNum));
//
//
//        for(int ii = 0; ii < splits.size(); ii++){
//            if(Arguments.PARTITIONS_TO_CHANGE.contains(ii) ){
//                //                 additionSplits.add(new PriorityFileSplit((FileSplit) splits.get(ii),GATKSAMPriorityInputFormat.VERY_HIGH_LEVEL));
//                // 如果在有效的最后5%内，最高优先级
//                if(ii>base && ii < validPartitionNum)
//                    additionSplits.addAll(getSmallSplits(ii, splits, 8,GATKSAMPriorityInputFormat.VERY_HIGH_LEVEL));
//                else
//                    additionSplits.addAll(getSmallSplits(ii, splits, 8,GATKSAMPriorityInputFormat.HIGH_LEVEL));
//            }else{
//                additionSplits.add(new PriorityFileSplit((FileSplit) splits.get(ii),GATKSAMPriorityInputFormat.LOW_LEVEL));
//            }
//        }

        // 根据优先级进行排序
        Collections.sort(additionSplits);
        // 将原本的清空，添加新的
        splits.clear();
        splits.addAll(additionSplits);



        final List<InputSplit> newSplits =
                new ArrayList<InputSplit>(splits.size());

        for (int i = 0; i < splits.size();) {
            try {
                i = addIndexedSplits      (splits, i, newSplits, cfg);
            } catch (IOException e) {
                i = addProbabilisticSplits(splits, i, newSplits, cfg);
            }
        }
        return filterByInterval(newSplits, cfg);
    }

}
