package org.scut.ccnl.genomics.io;


import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.log4j.Logger;
import org.scut.ccnl.genomics.Constants;

import java.io.Reader;
import java.util.Properties;

public class MysqlDAO {
    final protected static Logger logger = Logger.getLogger(MysqlDAO.class);
    // 采用静态内部类,通过JVM的机制实现SqlSessionFactory单例
    private static SqlSessionFactory sqlSessionFactory;
    static {
        logger.info("init SqlSessionFactory");
        try(Reader reader = Resources.getResourceAsReader("Configuration.xml")) {
            Properties properties = new Properties();
            // 从环境变量中获取属性,为了在Spark多节点中使用
            properties.setProperty("url",System.getProperty(Constants.MYSQL_URL));
            properties.setProperty("username",System.getProperty(Constants.MYSQL_USERNAME));
            properties.setProperty("password",System.getProperty(Constants.MYSQL_PASSWORD));
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader,properties);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static SqlSessionFactory getSqlSessionFactory(){
            return sqlSessionFactory;
        }


}
