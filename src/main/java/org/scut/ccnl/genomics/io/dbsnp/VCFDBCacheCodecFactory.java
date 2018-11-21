package org.scut.ccnl.genomics.io.dbsnp;

import htsjdk.samtools.util.PeekableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RODRecordListImpl;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.broadinstitute.gatk.utils.refdata.utils.RODRecordList;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.GlobalArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class VCFDBCacheCodecFactory extends VCFDBCodec {
    final private int AHEAD_NUM = 100_000;

    final private VCFDBCodec vcfdbCodec;
    private PeekableIterator<GATKFeature> peekableIterator = null;

    private VCFDBCacheCodecFactory(VCFDBCodec vcfdbCodec){
        this.vcfdbCodec = vcfdbCodec;
    }


    final protected static Logger logger = Logger.getLogger(VCFDBCacheCodecFactory.class);
    public static VCFDBCodec getVCFDBCodec(GlobalArgument globalArgument){
        return new VCFDBCacheCodecFactory(VCFDBCodecFactory.getVCFDBCodec(globalArgument));
    }

    @Override
    public void close() {
        vcfdbCodec.close();
    }

    @Override
    public RefMetaDataTracker parserLocation(GenomeLoc loc, GenomeLocParser parser) {
        // 避免超出染色体长度
        final int maxLen = parser.getContigInfo(loc.getContig()).getSequenceLength();
        // 如果迭代器空了
        if(peekableIterator == null || !peekableIterator.hasNext()){
           peekableIterator = new PeekableIterator<>(
                    vcfdbCodec.getGATKFeatures(
                            parser.createGenomeLoc(loc.getContig(),loc.getStart(),
                                    loc.getStop()+AHEAD_NUM>maxLen?maxLen:loc.getStop()+AHEAD_NUM),parser)
                            .iterator());
        }
        // 如果还是没有内容，返回空的
        if(!peekableIterator.hasNext()) {
            return new RefMetaDataTracker();
        }
        List<GATKFeature> features = new ArrayList<>();
        int start = loc.getStart();
        int stop = loc.getStop();

        GATKFeature temp = peekableIterator.peek();
        Integer last = null;

        while (temp.getStart() <= stop){
            // 需要对peekableIterator中部分数据 pass
            if(temp.getStart() < start){
                peekableIterator.next();
            }else{
                features.add(peekableIterator.next());
            }
            // 判断是否需要读
            if(!peekableIterator.hasNext()){
                // 看是否需要是接着上一次，还是重新开始
                int newStart;
                if(start > temp.getStart()){
                    newStart = start;
                }else{
                    if(last==null)
                        newStart =temp.getStart() + 1;
                    else
                        newStart = last + 1;
                }
                last = loc.getStop()+AHEAD_NUM>maxLen?maxLen:loc.getStop()+AHEAD_NUM;
                peekableIterator = new PeekableIterator<>(
                        vcfdbCodec.getGATKFeatures(
                                parser.createGenomeLoc(loc.getContig(),newStart,last),parser)
                                .iterator());
            }
            if(peekableIterator.hasNext()) {
                temp = peekableIterator.peek();
            }else{
                peekableIterator = null;
                break;

            }
        }

        RODRecordList rodRecordList = new RODRecordListImpl(Constants.DBSNP_NAME, features, loc);
        return new RefMetaDataTracker(Arrays.asList(rodRecordList));
    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser) {
        throw new UnsupportedOperationException();
    }
}
