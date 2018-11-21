// Copyright (c) 2010 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// File created: 2010-08-03 11:50:19

package org.seqdoop.hadoop_bam;

import org.apache.log4j.Logger;
import org.scut.ccnl.genomics.Arguments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for BAM files. Values
 * are the individual records; see {@link BAMRecordReader} for the meaning of
 * the key.
 */
public class GATKSAMPreprocessInputFormat
        extends GATKSAMInputFormat
{
    final protected static Logger logger = Logger.getLogger(GATKSAMPreprocessInputFormat.class);
    // set this to true for debug output

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

        // 仅对最后几个的分块进行处理
        int changesize = 0;
        int splits_size = splits.size();
        if(Arguments.CHANGE_PARTITION_NUM>=1)
            changesize = Math.min(splits_size, (int)Arguments.CHANGE_PARTITION_NUM);

        if(changesize <0) changesize = 0;

        System.out.println("\n预处理块数量:"+changesize);

        final List<InputSplit> newSplits =
                new ArrayList<InputSplit>(splits.size());

        for (int i = splits_size - changesize; i < splits_size;) {
            try {
                i = addIndexedSplits      (splits, i, newSplits, cfg);
            } catch (IOException e) {
                i = addProbabilisticSplits(splits, i, newSplits, cfg);
            }
        }
        return filterByInterval(newSplits, cfg);
    }


}
