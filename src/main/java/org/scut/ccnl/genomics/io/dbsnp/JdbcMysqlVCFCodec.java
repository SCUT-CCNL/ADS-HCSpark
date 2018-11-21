package org.scut.ccnl.genomics.io.dbsnp;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RODRecordListImpl;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.broadinstitute.gatk.utils.refdata.utils.RODRecordList;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.GlobalArgument;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class JdbcMysqlVCFCodec  extends VCFDBCodec {
    @Override
    public void close() {

    }
    public JdbcMysqlVCFCodec(GlobalArgument globalArgument){

    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser){
        ResultSet rs = null;
        PreparedStatement ps = null;
        List<GATKFeature> features = new ArrayList<>();

        try {
            int start = loc.getStart();
            int end = loc.getStop();
            String sql = "select chrom,pos,rs_id,ref,alt from "+"dbsnp"+" where chrom='"+loc.getContig()+"' and pos>="+start +" and pos<="+end;
            ps = JdbcMysqlConnection.getConnection().prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                features.add(new GATKFeature.TribbleGATKFeature(parser,parseVCFfromDB(rs), Constants.DBSNP_NAME));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return features;
    }

    public VariantContext parseVCFfromDB(ResultSet rs){
        VariantContextBuilder builder = new VariantContextBuilder();
        try {
            // 顺序为chrom | pos     | rs_id       | ref       | alt       | qual | filter | info
            builder.source(Constants.DBSNP_NAME);
            builder.chr(rs.getString(1));
            int pos = rs.getInt(2);
            builder.start(pos);

            final String ref = rs.getString(4);
            // TODO: 2017/6/9 为什么要减一？DBsnpTools里也是这样的
            builder.stop(pos+ref.length()-1);

            final String id = rs.getString(3);
            if (id.equals(VCFConstants.EMPTY_ID_FIELD)) builder.noID();
            else builder.id(id);
            // parseAlleles方法是抽象类的方法，protected域
            builder.alleles(parseAlleles(ref, rs.getString(5), 0));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return builder.make();
    }

    static class JdbcMysqlConnection{
        public static Connection getConnection() {
            return connection;
        }

        private static Connection connection;

        static {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                String url = "jdbc:mysql://cu01:3306/dbsnp" ;
                String username = "root" ;
                String password = "spacedb" ;

                connection = DriverManager.getConnection(url , username , password ) ;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
