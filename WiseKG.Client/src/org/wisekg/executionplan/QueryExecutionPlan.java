package org.wisekg.executionplan;

import org.wisekg.model.TriplePattern;
import org.wisekg.util.Tuple;
import org.rdfhdt.hdt.util.StarString;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryExecutionPlan {
    private final QueryOperator operator;
    private final QueryExecutionPlan subplan;
    private final long timestamp;

    public QueryExecutionPlan(QueryOperator operator, QueryExecutionPlan subplan, long timestamp) {
        this.operator = operator;
        this.subplan = subplan;
        this.timestamp = timestamp;
    }

    private QueryExecutionPlan() {
        this.operator = null;
        this.subplan = null;
        this.timestamp = 0;
    }

    public static QueryExecutionPlan getNullPlan() {
        return new QueryExecutionPlan();
    }

    public boolean isNullPlan() {
        return (operator == null) && (subplan == null);
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public QueryExecutionPlan getSubplan() {
        return subplan;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<TriplePattern> getTriples() {
        List<TriplePattern> lst = subplan.getTriples();
        lst.addAll(operator.getTriples());
        return lst;
    }

    public Set<String> getVariables() {
        if(operator == null) return new HashSet<>();
        Set<String> ret = new HashSet<>();
        StarString star = operator.getStar();
        String subj = star.getSubject().toString();
        if(subj.startsWith("?")) ret.add(subj);
        List<Tuple<CharSequence, CharSequence>> lst = star.getTriples();
        for(Tuple<CharSequence, CharSequence> tpl : lst) {
            String pred = tpl.first.toString();
            if(pred.startsWith("?")) ret.add(pred);
            String obj = tpl.second.toString();
            if(obj.startsWith("?")) ret.add(obj);
        }

        if(subplan == null) return ret;
        ret.addAll(subplan.getVariables());
        return ret;
    }
}
