package org.scut.ccnl.genomics.io.dbsnp;

import org.apache.log4j.Logger;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.GlobalArgument;


public class VCFDBCodecFactory {
    final protected static Logger logger = Logger.getLogger(VCFDBCodecFactory.class);
    public static VCFDBCodec getVCFDBCodec(GlobalArgument globalArgument){
        switch (globalArgument.DBSNP_DB){
            case Constants.STR_MYSQL:
                logger.info("mysql");
                return new MysqlVCFCodec();
            case Constants.STR_JDBCMYSQL:
                return new JdbcMysqlVCFCodec(globalArgument);
            case Constants.STR_HBASE:
                logger.info("hbase");
                return new HbaseVCFCodec();
            case Constants.STR_IMPALA:
                logger.info("impala:"+globalArgument.IMPALA_DBSNP);
                return new ImpalaVCFCodec(globalArgument);
            case Constants.STR_KUDU:
                logger.info("kudu");
                return new KuduVCFCodec(globalArgument);
            case Constants.STR_NFS:
                return new NfsVCFCodec(globalArgument);
            default:
                logger.info("none");
                return new NoneVCFDBCodec();

        }
    }

}
