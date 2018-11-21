package org.scut.ccnl.genomics.io.dbsnp;


import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RODRecordListImpl;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.broadinstitute.gatk.utils.refdata.utils.RODRecordList;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.GlobalArgument;

import java.io.Closeable;
import java.io.Reader;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ImpalaVCFCodec extends VCFDBCodec{

    public final static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static String CONNECTION_URL = "jdbc:hive2://192.168.0.9:21053/;auth=noSasl";

    private String tablename;
    Connection connection;


    public ImpalaVCFCodec(GlobalArgument globalArgument){

        tablename = globalArgument.IMPALA_DBSNP;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
    public ImpalaVCFCodec(String tablename){
        this.tablename = tablename;

    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser) {
        List<GATKFeature> features = new ArrayList<>();

        try {
            ResultSet rs = null;
            PreparedStatement ps = null;
            int start = loc.getStart();
            int end = loc.getStop();
            String sql = "select chrom,pos,rs_id,ref,alt from "+tablename+" where chrom='"+loc.getContig()+"' and pos>="+start +" and pos<="+end;
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                features.add(new GATKFeature.TribbleGATKFeature(parser,parseVCFfromDB(rs), Constants.DBSNP_NAME));
            }
            rs.close();
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

//    static class ImpalaConncetion{
//        private static SqlSessionFactory sqlSessionFactory;
//        static {
//            try(Reader reader = Resources.getResourceAsReader("impala.xml")) {
//                Properties properties = new Properties();
//                // 从环境变量中获取属性,为了在Spark多节点中使用
//                properties.setProperty("url",System.getProperty(Constants.IMPALA_URL));
//                properties.setProperty("driver",ImpalaVCFCodec.JDBC_DRIVER);
//                sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader,properties);
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//        public static SqlSessionFactory getSqlSessionFactory(){
//            return sqlSessionFactory;
//        }
//
//    }
}


