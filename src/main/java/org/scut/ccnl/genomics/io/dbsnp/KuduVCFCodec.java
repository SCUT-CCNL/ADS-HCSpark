package org.scut.ccnl.genomics.io.dbsnp;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
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

public class KuduVCFCodec  extends VCFDBCodec {
    private static final String KUDU_MASTER = System.getProperty(
            "kuducu10", "192.168.0.10:7051");

    private static final List<String> columns = Arrays.asList("chrom","pos","rs_id","ref","alt");

    public KuduVCFCodec(GlobalArgument globalArgument){

    }

    @Override
    public void close() {

    }

    public VariantContext parseVCFfromDB(RowResult rs){
        VariantContextBuilder builder = new VariantContextBuilder();
        try {
            // 顺序为chrom | pos     | rs_id       | ref       | alt       | qual | filter | info
            builder.source(Constants.DBSNP_NAME);
            builder.chr(rs.getString("chrom"));
            int pos = rs.getInt("pos");
            builder.start(pos);

            final String ref = rs.getString("ref");
            // TODO: 2017/6/9 为什么要减一？DBsnpTools里也是这样的
            builder.stop(pos+ref.length()-1);

            final String id = rs.getString("rs_id");
            if (id.equals(VCFConstants.EMPTY_ID_FIELD)) builder.noID();
            else builder.id(id);
            // parseAlleles方法是抽象类的方法，protected域
            builder.alleles(parseAlleles(ref, rs.getString("alt"), 0));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return builder.make();
    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser){
        List<GATKFeature> features = new ArrayList<>();
        try {
            KuduPredicate chromPredicate = KuduPredicate.newComparisonPredicate(
                    KuduConnection.getChromColumn(), KuduPredicate.ComparisonOp.EQUAL, loc.getContig());
            KuduPredicate startPredicate = KuduPredicate.newComparisonPredicate(
                    KuduConnection.getPosColumn(), KuduPredicate.ComparisonOp.GREATER_EQUAL, loc.getStart());
            KuduPredicate stopPredicate = KuduPredicate.newComparisonPredicate(
                    KuduConnection.getPosColumn(), KuduPredicate.ComparisonOp.LESS_EQUAL, loc.getStop());

            KuduScanner scanner = KuduConnection.getScannerBuilder().addPredicate(chromPredicate).
                    addPredicate(startPredicate).addPredicate(stopPredicate).setProjectedColumnNames(columns)
                    .build();
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    features.add(new GATKFeature.TribbleGATKFeature(parser,parseVCFfromDB(row), Constants.DBSNP_NAME));
                }
            }
            scanner.close();

        }catch (Exception e){
            e.printStackTrace();
        }
        return features;
    }

    static class KuduConnection{
        private static KuduClient client;
        private static KuduTable table;
        private static Schema schema;
        private static ColumnSchema chromColumn;
        private static ColumnSchema posColumn;


        static {
            try {
                client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
                table = client.openTable(Constants.DBSNP_KUDU_NAME);
                schema = table.getSchema();
                chromColumn = schema.getColumn("chrom");
                posColumn = schema.getColumn("pos");
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }

        public static KuduClient getClient() {
            return client;
        }

        public static KuduTable getTable() {
            return table;
        }

        public static Schema getSchema() {
            return schema;
        }

        public static ColumnSchema getChromColumn() {
            return chromColumn;
        }

        public static ColumnSchema getPosColumn() {
            return posColumn;
        }

        public static KuduScanner.KuduScannerBuilder getScannerBuilder(){
            return client.newScannerBuilder(table);
        }
    }
}
