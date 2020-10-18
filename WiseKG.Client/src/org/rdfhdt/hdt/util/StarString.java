package org.rdfhdt.hdt.util;

import org.wisekg.model.TriplePattern;
import org.wisekg.util.Tuple;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StarString {
    private CharSequence subject;
    private List<Tuple<CharSequence, CharSequence>> triples = new ArrayList<>();
    private ValueFactory valueFactory = SimpleValueFactory.getInstance();
    private final String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";

    /**
     * Basic constructor
     */
    public StarString() {
        super();
    }

    public StarString(CharSequence subject, List<Tuple<CharSequence, CharSequence>> triples) {
        this.subject = subject;

        for (Tuple<CharSequence, CharSequence> tpl : triples) {
            this.triples.add(new Tuple<>(tpl.first, tpl.second));
        }
    }

    public StarString(CharSequence subject) {
        this.subject = subject;
    }

    /**
     * Build a TripleID as a copy of another one.
     *
     * @param other
     */
    public StarString(StarString other) {
        super();
        subject = other.subject;
        triples = other.triples;
    }

    public TripleString getTriple(int pos) {
        Tuple<CharSequence, CharSequence> t = triples.get(pos);
        return new TripleString(subject, t.first, t.second);
    }

    public ArrayList<TripleString> toTripleStrings() {
        ArrayList<TripleString> ret = new ArrayList<>();
        for (Tuple<CharSequence, CharSequence> t : triples) {
            ret.add(new TripleString(subject, t.first, t.second));
        }
        return ret;
    }

    public List<CharSequence> getPredicates() {
        List<CharSequence> ret = new ArrayList<>();

        for (Tuple<CharSequence, CharSequence> t : triples) {
            ret.add(t.first);
        }

        return ret;
    }

    public List<Statement> getAsStatements() {
        List<Statement> stmts = new ArrayList<>();
        Resource subj = valueFactory.createIRI(subject.toString());

        for (Tuple<CharSequence, CharSequence> tpl : triples) {
            IRI pred = valueFactory.createIRI(tpl.first.toString());

            String objString = tpl.second.toString();
            Value obj;
            if (objString.matches(regex))
                obj = valueFactory.createIRI(objString);
            else
                obj = valueFactory.createLiteral(objString);

            stmts.add(valueFactory.createStatement(subj, pred, obj));
        }

        return stmts;
    }

    public int size() {
        return triples.size();
    }

    public CharSequence getSubject() {
        return subject;
    }

    public void setSubject(CharSequence subject) {
        this.subject = subject;
    }

    public List<Tuple<CharSequence, CharSequence>> getTriples() {
        return triples;
    }

    public void setTriples(List<Tuple<CharSequence, CharSequence>> triples) {
        this.triples = triples;
    }

    public void addTriple(Tuple<CharSequence, CharSequence> triple) {
        triples.add(triple);
    }

    public StarID toStarID(Dictionary dictionary) {
        int subj = subject.charAt(0) == '?' ? 0 : (int) dictionary.stringToId(subject, TripleComponentRole.SUBJECT);
        String subjVar = subject.charAt(0) == '?' ? subject.toString() : "";

        List<Tuple<String, String>> vars = new ArrayList<>();
        List<Tuple<Integer, Integer>> lst = new ArrayList<>();
        int size = size();
        for (int i = 0; i < size; i++) {
            TripleString tpl = getTriple(i);
            Tuple<Integer, Integer> t = new Tuple<>(
                    tpl.getPredicate().charAt(0) == '?' ? 0 : (int) dictionary.stringToId(tpl.getPredicate(), TripleComponentRole.PREDICATE),
                    tpl.getObject().charAt(0) == '?' ? 0 : (int) dictionary.stringToId(tpl.getObject(), TripleComponentRole.OBJECT)
            );
            lst.add(t);

            Tuple<String, String> t1 = new Tuple<>(
                    tpl.getPredicate().charAt(0) == '?' ? tpl.getPredicate().toString() : "",
                    tpl.getObject().charAt(0) == '?' ? tpl.getObject().toString() : ""
            );
            vars.add(t1);
        }

        return new StarID(subj, lst, subjVar, vars);
    }

    public void updateField(String name, String val) {
        if (name.equals(subject.toString())) {
            subject = val;
            return;
        }

        for (Tuple<CharSequence, CharSequence> tpl : triples) {
            if (name.equals(tpl.first.toString()))
                tpl.first = val;
            if (name.equals(tpl.second.toString()))
                tpl.second = val;
        }
    }

    @Override
    public String toString() {
        String str = subject.toString();

        for (Tuple<CharSequence, CharSequence> t : triples) {
            str += "\n    " + t.first + " " + t.second;
        }

        return str;
    }

    public List<String> getVariables() {
        List<String> vars = new ArrayList<>();
        if (subject.toString().startsWith("?"))
            vars.add(subject.toString());

        for (Tuple<CharSequence, CharSequence> tpl : triples) {
            String pred = tpl.first.toString(), obj = tpl.second.toString();
            if (pred.startsWith("?")) vars.add(pred);
            if (obj.startsWith("?")) vars.add(obj);
        }

        return vars;
    }

    public List<TriplePattern> getTriplePatterns() {
        List<TriplePattern> lst = new ArrayList<>();

        int i = 0;
        Var subj;
        if (subject.toString().startsWith("?"))
            subj = new Var(subject.toString());
        else {
            subj = new Var("var" + i, SimpleValueFactory.getInstance().createIRI(subject.toString()));
            i++;
        }

        for (Tuple<CharSequence, CharSequence> tpl : triples) {
            Var pred, obj;

            String p = tpl.first.toString(), o = tpl.second.toString();
            if (p.startsWith("?"))
                pred = new Var(p);
            else{
                pred = new Var("var" + i, SimpleValueFactory.getInstance().createIRI(p));
                i++;
            }

            if (o.startsWith("?"))
                obj = new Var(o);
            else{
                Value v;
                if(o.startsWith("http")) {
                    v = SimpleValueFactory.getInstance().createIRI(o);
                } else
                    v = SimpleValueFactory.getInstance().createLiteral(o);

                obj = new Var("var" + i, v);
                i++;
            }

            lst.add(new TriplePattern(new StatementPattern(subj, pred, obj)));

        }

        return lst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarString starID = (StarString) o;
        return subject == starID.subject &&
                Objects.equals(triples, starID.triples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, triples);
    }

    /**
     * Set all components to zero.
     */
    public void clear() {
        triples.clear();
        subject = "";
    }

    public String getVarString(String varName) {
        if (varName.equals(subject.toString()))
            return "subject";
        for (int i = 0; i < triples.size(); i++) {
            int j = i + 1;
            Tuple<CharSequence, CharSequence> tpl = triples.get(i);
            if (varName.equals(tpl.first.toString())) {
                return "p" + j;
            } else if (varName.equals(tpl.second.toString())) {
                return "o" + j;
            }
        }
        return "";
    }
}
