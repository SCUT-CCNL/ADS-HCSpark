package org.scut.ccnl.genomics.io.dbsnp;

/**
 * Created by shedfree on 2017/3/19.
 */
public class ImpalaDBsnp {


    private String chrom;
    private int pos;
    private String rs_id;
    private String ref;
    private String alt;
    private String qual;
    private String filter;
    private String info;

    public String getChrom() {
        return chrom;
    }

    public void setChrom(String chrom) {
        this.chrom = chrom;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public String getRs_id() {
        return rs_id;
    }

    public void setRs_id(String rs_id) {
        this.rs_id = rs_id;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getAlt() {
        return alt;
    }

    public void setAlt(String alt) {
        this.alt = alt;
    }

    public String getQual() {
        return qual;
    }

    public void setQual(String qual) {
        this.qual = qual;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    @Override
    public String toString() {
        return "ImpalaDBsnp{" +
                "chrom='" + chrom + '\'' +
                ", pos=" + pos +
                ", rs_id='" + rs_id + '\'' +
                ", ref='" + ref + '\'' +
                ", alt='" + alt + '\'' +
                ", qual='" + qual + '\'' +
                ", filter='" + filter + '\'' +
                ", info='" + info + '\'' +
                '}';
    }
}
