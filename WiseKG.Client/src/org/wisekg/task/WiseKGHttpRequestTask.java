package org.wisekg.task;

import com.github.jsonldjava.shaded.com.google.common.collect.Sets;
import org.wisekg.executionplan.QueryExecutionPlan;
import org.wisekg.executionplan.QueryOperator;
import org.wisekg.model.BindingHashMap;
import org.wisekg.model.HttpRequestConfig;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StarString;

import java.util.*;
import java.util.concurrent.*;

public class WiseKGHttpRequestTask {
    private QueryExecutionPlan originalPlan;
    private QueryExecutionPlan plan;
    private QueryOperator operator;
    private ArrayList<BindingHashMap> bindings;
    private String fragmentURL;
    private ConcurrentLinkedQueue<BindingHashMap> outputBindings;
    private static URLCodec urlCodec = new URLCodec("utf8");
    private boolean single = false;

    public WiseKGHttpRequestTask(ArrayList<BindingHashMap> bindings,
                                 ConcurrentLinkedQueue<BindingHashMap> outputBindings,
                                 QueryExecutionPlan plan) {
        this.operator = getOperator(plan);

        if(this.operator != null && this.operator.getStar().size() == 1) this.single = true;

        this.originalPlan = plan;
        this.plan = removeLast(plan);

        this.bindings = bindings;
        this.outputBindings = outputBindings;
        try {
            this.fragmentURL = constructURL();
        } catch (EncoderException e) {
            e.printStackTrace();
        }
    }

    public WiseKGHttpRequestTask(ArrayList<BindingHashMap> bindings,
                                 ConcurrentLinkedQueue<BindingHashMap> outputBindings,
                                 QueryExecutionPlan plan, String fragmentURL) {
        this.operator = getOperator(plan);

        if(this.operator != null && this.operator.getStar().size() == 1) this.single = true;

        this.originalPlan = plan;

        this.plan = removeLast(plan);

        this.bindings = bindings;
        this.outputBindings = outputBindings;
        this.fragmentURL = fragmentURL;
    }

    public QueryExecutionPlan getPlan() {
        return originalPlan;
    }

    public void setPlan(QueryExecutionPlan plan) {
        this.operator = getOperator(plan);

        if(this.operator != null && this.operator.getStar().size() == 1) this.single = true;

        this.originalPlan = plan;
        this.plan = removeLast(plan);
    }

    public QueryExecutionPlan getSubplan() {
        return plan;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    private QueryOperator getOperator(QueryExecutionPlan plan) {
        if (plan.isNullPlan()) return null;
        if (plan.getSubplan().isNullPlan()) return plan.getOperator();
        return getOperator(plan.getSubplan());
    }

    private QueryExecutionPlan removeLast(QueryExecutionPlan plan) {
        if (plan.isNullPlan()) return plan;
        if (plan.getSubplan().isNullPlan()) return QueryExecutionPlan.getNullPlan();
        return new QueryExecutionPlan(plan.getOperator(), removeLast(plan.getSubplan()), plan.getTimestamp());
    }

    public boolean isSingle() {
        return single;
    }


    public ArrayList<BindingHashMap> getBindings() {
        return bindings;
    }

    private String constructURLSingle() throws EncoderException {
        TripleString tp = operator.getStar().getTriple(0);
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();

        isQuestionMarkAdded = appendUrlParamSingle(sb, tp.getSubject().toString(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendUrlParamSingle(sb, tp.getPredicate().toString(),
                HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
        isQuestionMarkAdded =
                appendUrlParamSingle(sb, tp.getObject().toString(), HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);
        if (!bindings.isEmpty()) {
            appendBindingsSingle(sb);
        }

        return operator.getControl() + sb.toString();
    }

    private void appendBindingsSingle(StringBuilder sb) throws EncoderException {
        if (!bindings.isEmpty()) {
            TripleString tp = operator.getStar().getTriple(0);
            Set<String> varsInTP = new HashSet<>(tp.getVariables());
            StringBuilder valuesSb = new StringBuilder();
            Set<String> boundVars = bindings.get(0).keySet();
            ArrayList<String> varsInURL = new ArrayList<String>(Sets.intersection(varsInTP, boundVars));
            List<String> vars = new ArrayList<>();
            for (String str : varsInURL) {
                vars.add("?" + tp.getVarString(str));
            }
            valuesSb.append("(");
            valuesSb.append(String.join(" ", vars));
            valuesSb.append("){");

            Set<ArrayList<String>> set = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                ArrayList<String> bindingsStrList = new ArrayList<String>();
                for (int j = 0; j < varsInURL.size(); j++) {
                    bindingsStrList.add(bindings.get(i).get(varsInURL.get(j)).toString());
                }
                if (set.contains(bindingsStrList)) continue;
                set.add(bindingsStrList);
                valuesSb.append("(");
                valuesSb.append(String.join(" ", bindingsStrList));
                valuesSb.append(")");
            }
            valuesSb.append("}");
            sb.append("&").append(HttpRequestConfig.BINDINGS_PARAM).append("=")
                    .append(urlCodec.encode(valuesSb.toString()));
        }
    }

    private boolean appendUrlParamSingle(StringBuilder sb, String var, String paramName,
                                         Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            sb.append("&").append(paramName).append("=")
                    .append(urlCodec.encode(var));
        } else {
            sb.append("?").append(paramName).append("=")
                    .append(urlCodec.encode(var));
            return true;
        }
        return isQuestionMarkAdded;
    }


    private String constructURL() throws EncoderException {
        if (single) return constructURLSingle();

        StarString sp = operator.getStar();
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();

        int num = sp.size();
        isQuestionMarkAdded = appendUrlParam(sb, sp.getSubject().toString(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendTripleParam(sb, num, HttpRequestConfig.TRIPLES_PARAM,
                isQuestionMarkAdded);

        String str = "[";
        for (int i = 0; i < num; i++) {
            int j = i + 1;
                str = str + "p" + j + "," + sp.getTriple(i).getPredicate() + ";";
                str = str + "o" + j + "," + sp.getTriple(i).getObject() + ";";
        }

        str = str.substring(0, str.length() - 1) + "]";
        isQuestionMarkAdded = appendStringParam(sb, str, "star", isQuestionMarkAdded);
        if (!bindings.isEmpty()) {
            appendBindings(sb);
        }


        //isQuestionMarkAdded = appendStringParam(sb, Experiment.cardinalities.get(this.tpIdx) + "", "card", isQuestionMarkAdded);
        return operator.getControl() + sb.toString();
    }

    private void appendBindings(StringBuilder sb) throws EncoderException {
        if (!bindings.isEmpty()) {
            StarString tp = operator.getStar();
            Set<String> varsInTP = new HashSet<>(tp.getVariables());
            StringBuilder valuesSb = new StringBuilder();
            Set<String> boundVars = bindings.get(0).keySet();
            ArrayList<String> varsInURL = new ArrayList<String>(Sets.intersection(varsInTP, boundVars));
            List<String> vars = new ArrayList<>();
            for (String str : varsInURL) {
                vars.add("?" + tp.getVarString(str));
            }
            valuesSb.append("(");
            valuesSb.append(String.join(" ", vars));
            valuesSb.append("){");

            Set<ArrayList<String>> set = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                ArrayList<String> bindingsStrList = new ArrayList<String>();
                for (int j = 0; j < varsInURL.size(); j++) {
                    bindingsStrList.add(bindings.get(i).get(varsInURL.get(j)).toString());
                }
                if (set.contains(bindingsStrList)) continue;
                set.add(bindingsStrList);
                valuesSb.append("(");
                valuesSb.append(String.join(" ", bindingsStrList));
                valuesSb.append(")");
            }
            valuesSb.append("}");
            sb.append("&").append(HttpRequestConfig.BINDINGS_PARAM).append("=")
                    .append(urlCodec.encode(valuesSb.toString()));
        }

    }

    private boolean appendUrlParam(StringBuilder sb, String var, String paramName,
                                   Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            sb.append("&").append(paramName).append("=")
                    .append(urlCodec.encode(var));
        } else {
            sb.append("?").append(paramName).append("=")
                    .append(urlCodec.encode(var));
            return true;
        }
        return isQuestionMarkAdded;
    }

    private boolean appendTripleParam(StringBuilder sb, int num, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(num);
        return isQuestionMarkAdded;
    }

    private boolean appendStringParam(StringBuilder sb, String str, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(urlCodec.encode(str));
        return isQuestionMarkAdded;
    }

    public String getFragmentURL() {
        return fragmentURL;
    }

    public StarString getStarPattern() {
        return operator.getStar();
    }

    public ConcurrentLinkedQueue<BindingHashMap> getOutputBindings() {
        return outputBindings;
    }

}
