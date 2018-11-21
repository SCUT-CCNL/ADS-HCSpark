package org.scut.ccnl.genomics.io;

import org.apache.commons.cli.CommandLine;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.GenomeLoc;
import org.broadinstitute.gatk.utils.GenomeLocParser;
import org.broadinstitute.gatk.utils.refdata.RefMetaDataTracker;
import org.broadinstitute.gatk.utils.refdata.utils.GATKFeature;
import org.scut.ccnl.genomics.Arguments;
import org.scut.ccnl.genomics.Constants;
import org.scut.ccnl.genomics.GlobalArgument;
import org.scut.ccnl.genomics.io.dbsnp.DBsnp;
import org.scut.ccnl.genomics.io.dbsnp.IDBsnpOperator;
import org.scut.ccnl.genomics.io.dbsnp.VCFDBCodec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;


public class DBsnpTools extends VCFDBCodec {
    final static Logger logger = Logger.getLogger(DBsnpTools.class);

    //运行 DBsnpTools 的基本参数
    public static boolean checkPram(CommandLine commandLine){
        if(!commandLine.hasOption("i")){
            logger.error("-i is wrong");
            return false;
        }
        if("".equals(Arguments.MYSQL_USERNAME)||
                "".equals(Arguments.MYSQL_URL)||
                "".equals(Arguments.MYSQL_PASSWORD)){
            logger.error("mysql url, username, password should not be null");
            return false;
        }

        return true;
    }

    public static void buildDbsnp(CommandLine commandLine){
        if(!checkPram(commandLine)) return;
        setEnviVariable(new GlobalArgument(true));

        try(BufferedReader reader= new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(commandLine.getOptionValue("i")))));
            SqlSession session = MysqlDAO.getSqlSessionFactory().openSession()) {

            IDBsnpOperator iDBsnpOperator = session.getMapper(IDBsnpOperator.class);

            //如果表不存在，则创建
            if(iDBsnpOperator.existTable()==0) {
                logger.info("table is not existed");
                iDBsnpOperator.createDbsnpTable();
                logger.info("create table successfully");
            }else {
                logger.info("table is existed");
            }

            int num = 0;
            String line =null;
            while((line=reader.readLine())!=null){

                if(line.startsWith("#")) continue;

                String[] sublines = line.split("\t");
                DBsnp dBsnp = new DBsnp();
                dBsnp.setChrom(sublines[0]);
                dBsnp.setPos(Integer.valueOf(sublines[1]));
                dBsnp.setStop(Integer.valueOf(sublines[1])+sublines[3].length()-1);
                dBsnp.setId(sublines[2]);
                dBsnp.setRef(sublines[3]);
                dBsnp.setAlt(sublines[4]);
                dBsnp.setQual(sublines[5]);
                dBsnp.setFilter(sublines[6]);
                dBsnp.setInfo(sublines[7]);

                try {
                    iDBsnpOperator.addDBsnp(dBsnp);
                }catch (Exception e){
                    System.out.println(dBsnp);
                    throw e;
                }
                if(num++%10000==0){//100000
                    session.commit();
                }
            }
            session.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setEnviVariable(GlobalArgument globalArgument){
        System.setProperty(Constants.MYSQL_URL, globalArgument.MYSQL_URL);
        System.setProperty(Constants.MYSQL_USERNAME, globalArgument.MYSQL_USERNAME);
        System.setProperty(Constants.MYSQL_PASSWORD, globalArgument.MYSQL_PASSWORD);

    }

    @Override
    public void close() {

    }

    @Override
    public RefMetaDataTracker parserLocation(GenomeLoc loc, GenomeLocParser parser) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GATKFeature> getGATKFeatures(GenomeLoc loc, GenomeLocParser parser) {
        throw new UnsupportedOperationException();
    }


}
