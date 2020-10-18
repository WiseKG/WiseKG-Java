package org.wisekg.model;

import org.wisekg.util.Tuple;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StarString;

import java.util.ArrayList;
import java.util.List;

public class StarPattern {
    private List<StatementPattern> statementPatterns;
    private ArrayList<String> listOfVars;
    private String subjectVarName;
    private List<Tuple<String, String>> varNames;
    private int triplesCount;

    public StarPattern(List<StatementPattern> statementPatterns) {
        this.statementPatterns = statementPatterns;
        this.listOfVars = new ArrayList<String>();
        subjectVarName = null;
        Var subjectVar = statementPatterns.get(0).getSubjectVar();
        if (!subjectVar.isAnonymous() && !subjectVar.isConstant()) {
            subjectVarName = "?" + subjectVar.getName();
            listOfVars.add(subjectVarName);
        }

        varNames = new ArrayList<>();

        for (StatementPattern pattern : statementPatterns) {
            Tuple<String, String> tpl = new Tuple<>(null, null);

            String pVarName = null;
            Var predicateVar = pattern.getPredicateVar();
            if (!predicateVar.isAnonymous() && !predicateVar.isConstant()) {
                pVarName = "?" + predicateVar.getName();
                listOfVars.add(pVarName);
            }
            if (pVarName != null)
                tpl.first = pVarName;

            String oVarName = null;
            Var objectVar = pattern.getObjectVar();
            if (!objectVar.isAnonymous() && !objectVar.isConstant()) {
                oVarName = "?" + objectVar.getName();
                listOfVars.add(oVarName);
            }
            if (oVarName != null)
                tpl.second = oVarName;

            varNames.add(tpl);
        }
    }

    public StarString getAsStarString() {
        List<Tuple<CharSequence, CharSequence>> list = new ArrayList<>();

        String subj;
        Var sVar = getSubjectVar();

        if(sVar.getValue() != null)
            subj = sVar.getValue().stringValue();
        else
            subj = "?"+sVar.getName();

        for(int i = 0; i < statementPatterns.size(); i++) {
            String pred, obj;
            Var pVar = getPredicateVar(i), oVar = getObjectVar(i);
            if(pVar.getValue() != null)
                pred = pVar.getValue().stringValue();
            else
                pred = "?"+pVar.getName();

            if(oVar.getValue() != null)
                obj = oVar.getValue().stringValue();
            else {
                String v = oVar.getName();
                if(v.startsWith("_anon_"))
                    obj = "?" + sVar.getName();
                else
                    obj = "?" + oVar.getName();
            }

            list.add(new Tuple<>(pred, obj));
        }
        return new StarString(subj, list);
    }

    public ArrayList<TripleString> getAsTripleStrings() {
        ArrayList<TripleString> list = new ArrayList<>();

        String subj;
        Var sVar = getSubjectVar();

        if(sVar.getValue() != null)
            subj = sVar.getValue().stringValue();
        else
            subj = "";

        for(int i = 0; i < statementPatterns.size(); i++) {
            String pred, obj;
            Var pVar = getPredicateVar(i), oVar = getObjectVar(i);
            if(pVar.getValue() != null)
                pred = pVar.getValue().stringValue();
            else
                pred = "";

            if(oVar.getValue() != null)
                obj = oVar.getValue().stringValue();
            else
                obj = "";

            list.add(new TripleString(subj, pred, obj));
        }
        return list;
    }

    public StatementPattern getStatement(int index) {
        return statementPatterns.get(index);
    }

    public String getVarString(String varName) {
        if(varName.equals(subjectVarName))
            return "subject";
        for (int i = 0; i < varNames.size(); i++) {
            int j = i+1;
            Tuple<String, String> tpl = varNames.get(i);
            if (varName.equals(tpl.first)) {
                return "p"+j;
            } else if (varName.equals(tpl.second)) {
                return "o"+j;
            }
        }
        return "";
    }

    public boolean containsVar(String varName) {
        return listOfVars.contains(varName);
    }

    public List<StatementPattern> getStatementPatterns() {
        return statementPatterns;
    }

    public int getNumberOfBoundVariables(ArrayList<String> boundVars) {
        int numberOfBV = 0;
        for (String boundVar : boundVars) {
            if (containsVar(boundVar)) {
                numberOfBV++;
            }
        }
        return numberOfBV;
    }

    public ArrayList<String> getListOfVars() {
        return listOfVars;
    }

    public Var getSubjectVar() {
        return statementPatterns.get(0).getSubjectVar();
    }

    public String getSubjectVarName() {
        return subjectVarName;
    }

    public int getNumberOfTriplePatterns() {
        return statementPatterns.size();
    }

    public int getTriplesCount() {
        return triplesCount;
    }

    public Var getPredicateVar(int index) {
        return statementPatterns.get(index).getPredicateVar();
    }

    public Var getObjectVar(int index) {
        return statementPatterns.get(index).getObjectVar();
    }

    public String getPredicateVarName(int index) {
        return varNames.get(index).first;
    }

    public String getObjectVarName(int index) {
        return varNames.get(index).second;
    }

    public void setTriplesCount(int triplesCount) {
        this.triplesCount = triplesCount;
    }

    public String getBgpString() {
        StringBuilder sb = new StringBuilder();

        String subj;
        Var sVar = getSubjectVar();

        if(sVar.getValue() != null)
            subj = sVar.getValue().stringValue();
        else
            subj = "?"+sVar.getName();

        for(int i = 0; i < statementPatterns.size(); i++) {
            String pred, obj;
            Var pVar = getPredicateVar(i), oVar = getObjectVar(i);
            if(pVar.getValue() != null)
                pred = pVar.getValue().stringValue();
            else
                pred = "?"+pVar.getName();

            if(oVar.getValue() != null)
                obj = oVar.getValue().stringValue();
            else {
                String v = oVar.getName();
                if(v.startsWith("_anon_"))
                    obj = "?" + sVar.getName();
                else
                    obj = "?" + oVar.getName();
            }

            sb.append(subj + " " + pred + " " + obj + " . ");
        }

        return sb.toString();
    }
}
