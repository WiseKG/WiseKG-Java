package org.wisekg.util;

public class Tuple<X, Y> {
    public X first;
    public Y second;

    public Tuple() {}
    public Tuple(X x, Y y) {
        this.first = x;
        this.second = y;
    }
}
