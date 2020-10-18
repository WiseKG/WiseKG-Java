package org.wisekg.executionplan;


import org.wisekg.model.TriplePattern;
import org.rdfhdt.hdt.util.StarString;

import java.util.List;
import java.util.Objects;

public class QueryOperator {
    private final String control;
    private final StarString star;

    public QueryOperator(String control, StarString star) {
        this.control = control;
        this.star = star;
    }

    public String getControl() {
        return control;
    }

    public StarString getStar() {
        return star;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryOperator that = (QueryOperator) o;
        return Objects.equals(control, that.control) &&
                Objects.equals(star, that.star);
    }

    public List<TriplePattern> getTriples() {

        return star.getTriplePatterns();
    }

    @Override
    public int hashCode() {
        return Objects.hash(control, star);
    }
}
