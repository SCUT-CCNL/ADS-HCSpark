package org.scut.ccnl.genomics.io;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import scala.Tuple2;

import java.io.IOException;


public class VariantsSparkSink {
    // Output format class for writing VCF files through saveAsNewAPIHadoopFile. Must be public.
    public static class SparkVCFOutputFormat extends KeyIgnoringVCFOutputFormat<NullWritable> {
        public static VCFHeader vcfHeader;

        public static void setVCFHeader(final VCFHeader header) {
            vcfHeader = header;
        }

        public SparkVCFOutputFormat() {
            super(VCFFormat.VCF);
        }

        @Override
        public RecordWriter<NullWritable, VariantContextWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException {
            setHeader(vcfHeader);
            // don't add an extension, since FileOutputFormat will add a compression extension automatically (e.g. .bgz)
            return super.getRecordWriter(ctx);
        }

        @Override
        public void checkOutputSpecs(JobContext job) throws IOException {
            try {
                super.checkOutputSpecs(job);
            } catch (FileAlreadyExistsException e) {
                // delete existing files before overwriting them
                final Path outDir = getOutputPath(job);
                outDir.getFileSystem(job.getConfiguration()).delete(outDir, true);
            }
        }
    }

    public static void saveAsShardedHadoopFiles(
            final JavaSparkContext ctx, final Configuration conf, final String outputFile, JavaRDD<VariantContext> variants,
            final VCFHeader header) throws IOException {
        // Set the static header on the driver thread.
        SparkVCFOutputFormat.setVCFHeader(header);

        final Broadcast<VCFHeader> headerBroadcast = ctx.broadcast(header);

        // SparkVCFOutputFormat is a static class, so we need to copy the header to each worker then call
        final JavaRDD<VariantContext> variantsRDD = setHeaderForEachPartition(variants, headerBroadcast);

        // The expected format for writing is JavaPairRDD where the key is ignored and the value is VariantContextWritable.
        final JavaPairRDD<VariantContext, VariantContextWritable> rddVariantContextWriteable = pairVariantsWithVariantContextWritables(variantsRDD);

        rddVariantContextWriteable.saveAsNewAPIHadoopFile(outputFile, VariantContext.class, VariantContextWritable.class, SparkVCFOutputFormat.class, conf);
    }

    private static JavaRDD<VariantContext> setHeaderForEachPartition(final JavaRDD<VariantContext> variants, final Broadcast<VCFHeader> headerBroadcast) {
        return variants.mapPartitions(iterator -> {
            SparkVCFOutputFormat.setVCFHeader(headerBroadcast.getValue());
            return iterator;
        });
    }

    private static JavaPairRDD<VariantContext, VariantContextWritable> pairVariantsWithVariantContextWritables(JavaRDD<VariantContext> records) {
        return records.mapToPair(variantContext -> {
            final VariantContextWritable variantContextWritable = new VariantContextWritable();
            variantContextWritable.set(variantContext);
            return new Tuple2<>(variantContext, variantContextWritable);
        });
    }

}
