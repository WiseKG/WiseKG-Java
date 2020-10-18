package org.linkeddatafragments.util;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.*;

import java.util.*;

public class BgpStarPatternVisitor implements ElementVisitor {
    private Map<String, StarString> map = new HashMap<>();

    public Map<String, StarString> getMap() {
        return map;
    }

    public Set<StarString> getStarPatterns() {
        return new HashSet<>(map.values());
    }

    @Override
    public void visit(ElementTriplesBlock elementTriplesBlock) {
        List<Triple> triples = elementTriplesBlock.getPattern().getList();
        for(Triple t : triples) {
            String subject = t.getSubject().isVariable()? "?" + t.getSubject().getName() : t.getSubject().getURI();
            String predicate = t.getPredicate().isVariable()? "?" + t.getPredicate().getName() : t.getPredicate().getURI();
            String object = t.getObject().isVariable()? "?" + t.getObject().getName() : (t.getObject().isLiteral()? t.getObject().getLiteral().toString() : t.getObject().getURI());
            if(!map.containsKey(subject))
                map.put(subject, new StarString(subject));
            map.get(subject).addTriple(new Tuple<>(predicate, object));
        }
    }

    @Override
    public void visit(ElementPathBlock elementPathBlock) {
        List<TriplePath> triples = elementPathBlock.getPattern().getList();
        for(TriplePath t : triples) {
            String subject = t.getSubject().isVariable()? "?" + t.getSubject().getName() : t.getSubject().getURI();
            String predicate = t.getPredicate().isVariable()? "?" + t.getPredicate().getName() : t.getPredicate().getURI();
            String object = t.getObject().isVariable()? "?" + t.getObject().getName() : (t.getObject().isLiteral()? t.getObject().getLiteral().toString() : t.getObject().getURI());
            if(!map.containsKey(subject))
                map.put(subject, new StarString(subject));
            map.get(subject).addTriple(new Tuple<>(predicate, object));
        }
    }

    @Override
    public void visit(ElementFilter elementFilter) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementAssign elementAssign) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementBind elementBind) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementData elementData) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementUnion elementUnion) {
        for(Element ele : elementUnion.getElements()) {
            ele.visit(this);
        }
    }

    @Override
    public void visit(ElementOptional elementOptional) {
        elementOptional.getOptionalElement().visit(this);
    }

    @Override
    public void visit(ElementGroup elementGroup) {
        for(Element ele : elementGroup.getElements()) {
            ele.visit(this);
        }
    }

    @Override
    public void visit(ElementDataset elementDataset) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementNamedGraph elementNamedGraph) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementExists elementExists) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementNotExists elementNotExists) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementMinus elementMinus) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementService elementService) {
        /* DO NOTHING */
    }

    @Override
    public void visit(ElementSubQuery elementSubQuery) {
        /* DO NOTHING */
    }
}
