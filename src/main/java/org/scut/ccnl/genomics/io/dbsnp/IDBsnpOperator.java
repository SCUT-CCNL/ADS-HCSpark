package org.scut.ccnl.genomics.io.dbsnp;


import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * Created by shedfree on 2017/3/20.
 */
public interface IDBsnpOperator {
    public DBsnp selectDBsnpByPos(@Param("chr") String chr, @Param("pos") int pos);

    public void addDBsnp(DBsnp dBsnp);

    public List<DBsnp> queryLoc(@Param("chr") String chr, @Param("pos") int pos);

    public List<DBsnp> queryByRange(@Param("chr") String chr, @Param("start") int start, @Param("stop") int stop);

    public Integer existTable();

    public void createDbsnpTable();
}
