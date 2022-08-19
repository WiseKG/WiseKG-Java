package org.linkeddatafragments.executionplan;

import org.linkeddatafragments.characteristicset.ICharacteristicSet;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.util.StarString;
import org.linkeddatafragments.util.Tuple;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorStarString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.ArrayList;
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

    public QueryExecutionPlan(QueryOperator operator, QueryExecutionPlan subplan) {
        this.operator = operator;
        this.subplan = subplan;
        this.timestamp = System.currentTimeMillis();
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

    public long cardinalityEstimation() {
        if(isNullPlan() || operator == null || subplan == null) return 0;
        List<ICharacteristicSet> characteristicSets = ConfigReader.getInstance().getCharacteristicSets();
        List<ICharacteristicSet> css = new ArrayList<>();
        StarString curr = operator.getStar();

        Set<String> vars = subplan.getVariables();
        long card = subplan.cardinalityEstimation();

        for (ICharacteristicSet cs : characteristicSets) {
            if (cs.matches(curr)) css.add(cs);
        }

        double size = 0;
        for (ICharacteristicSet cs : css) {
            size += cs.count(curr, vars, card);
        }

        return (long) size;
    }

    public long cardinalityEstimation(IDataSource dataSource) {
        if(operator == null) return 0;
        return dataSource.cardinalityEstimation(operator.getStar());
    }

    public static long cardinalityEstimation(StarString curr, QueryExecutionPlan subplan, IDataSource datasource) {
        List<ICharacteristicSet> characteristicSets = ConfigReader.getInstance().getCharacteristicSets();
        List<ICharacteristicSet> css = new ArrayList<>();

        Set<String> vars = subplan.getVariables();
        long card = subplan.cardinalityEstimation();

        for (ICharacteristicSet cs : characteristicSets) {
            if (cs.matches(curr)) css.add(cs);
        }

        double size = 0;
        for (ICharacteristicSet cs : css) {
            size += cs.count(curr, vars, card);
        }

        return (long) size;
    }

    public static long cardinalityEstimation(StarString curr, QueryExecutionPlan subplan) {
        List<ICharacteristicSet> characteristicSets = ConfigReader.getInstance().getCharacteristicSets();
        List<ICharacteristicSet> css = new ArrayList<>();

        Set<String> vars = subplan.getVariables();
        long card = subplan.cardinalityEstimation();

        for (ICharacteristicSet cs : characteristicSets) {
            if (cs.matches(curr)) css.add(cs);
        }

        double size = 0;
        for (ICharacteristicSet cs : css) {
            size += cs.count(curr, vars, card);
        }

        return (long) size;
    }

    public Set<String> getVariables() {
        if(operator == null) return new HashSet<>();
        Set<String> ret = new HashSet<>();
        StarString star = operator.getStar();
        String subj = star.getSubject().toString();
        if(subj.startsWith("?")) ret.add(subj);
        List<Tuple<CharSequence, CharSequence>> lst = star.getTriples();
        for(Tuple<CharSequence, CharSequence> tpl : lst) {
            String pred = tpl.x.toString();
            if(pred.startsWith("?")) ret.add(pred);
            String obj = tpl.y.toString();
            if(obj.startsWith("?")) ret.add(obj);
        }

        if(subplan == null) return ret;
        ret.addAll(subplan.getVariables());
        return ret;
    }
}
