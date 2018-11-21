package org.scut.ccnl.genomics.io.dbsnp;


import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * Created by shedfree on 2017/3/20.
 */
public interface ImpalasnpOperator {

    public List<ImpalaDBsnp> queryByRange(@Param("tablename") String tablename,
                                          @Param("chr") String chr, @Param("start") int start, @Param("stop") int stop);
}
