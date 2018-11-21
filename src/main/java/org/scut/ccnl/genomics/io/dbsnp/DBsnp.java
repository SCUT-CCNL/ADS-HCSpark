package org.scut.ccnl.genomics.io.dbsnp;

/**
 * Created by shedfree on 2017/3/19.
 */
public class DBsnp {


    private String chrom;
    private int pos;
    private int stop;
    private String id;
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

    public int getStop() {
        return stop;
    }

    public void setStop(int stop) {
        this.stop = stop;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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
        return "DBsnp{" +
                "chrom='" + chrom + '\'' +
                ", pos=" + pos +
                ", stop=" + stop +
                ", id='" + id + '\'' +
                ", ref='" + ref + '\'' +
                ", alt='" + alt + '\'' +
                '}';
    }
}
