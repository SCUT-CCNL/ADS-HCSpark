package org.scut.ccnl.genomics;

/**
 * Created by shedfree on 2018/1/22.
 */
public class Tuple4<T1,T2,T3,T4> {
    public T1 _1;
    public T2 _2;
    public T3 _3;
    public T4 _4;

    public Tuple4(T3 _3, T4 _4) {
        this._3 = _3;
        this._4 = _4;
    }

    public Tuple4(T1 _1, T2 _2, T3 _3, T4 _4) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
        this._4 = _4;
    }

    @Override
    public String toString() {
        return "Tuple4{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                ", _3=" + _3 +
                ", _4=" + _4 +
                '}';
    }
}
