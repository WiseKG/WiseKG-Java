package org.wisekg.callable;

import org.wisekg.model.BindingHashMap;
import org.wisekg.task.StarHandlerHdt;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorStarString;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StarString;

import java.util.ArrayList;
import java.util.Iterator;

public class HdtProcessorStar {
    private final AbstractRDFHandler handler;
    private final StarString star;
    private final HDT hdt;
    private final ArrayList<BindingHashMap> bindings;
    private final String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";

    public HdtProcessorStar(AbstractRDFHandler handler, StarString star, HDT hdt, ArrayList<BindingHashMap> bindings) {
        this.handler = handler;
        this.star = star;
        this.hdt = hdt;
        this.bindings = bindings;
    }

    public void process() {
        if(star.size() == 1) {
            processSingle();
            return;
        }

        IteratorStarString it = hdt.searchStarBindings(star, bindings);
        while (it.hasNext()) {
            ((StarHandlerHdt) handler).handleStatements(it.next().getAsStatements());
        }

        handler.endRDF();
    }

    private void processSingle() {
        StarStringIterator iterator = new StarStringIterator(bindings, star);
        while(iterator.hasNext()) {
            StarString st = iterator.next();
            TripleString triple = st.getTriple(0);
            try {
                IteratorTripleString it = hdt.search(
                        triple.getSubject().toString().startsWith("?") ? "" : triple.getSubject(),
                        triple.getPredicate().toString().startsWith("?") ? "" : triple.getPredicate(),
                        triple.getObject().toString().startsWith("?") ? "" : triple.getObject());
                while (it.hasNext()) {
                    handler.handleStatement(it.next().getAsStatement());
                }
            } catch (NotFoundException e) {
                return;
            }
        }

        handler.endRDF();
    }

    private static class StarStringIterator implements Iterator<StarString> {
        private ArrayList<BindingHashMap> bindings;
        private StarString star;
        private int current = 0;
        private StarString next = null;

        StarStringIterator(ArrayList<BindingHashMap> bindings, StarString star) {
            this.bindings = bindings;
            this.star = star;
        }

        private void bufferNext() {
            if((bindings == null || bindings.size() == 0) && current == 0) {
                next = star;
                current++;
                return;
            }
            if(bindings == null) {
                next = null;
                return;
            }
            if (current >= bindings.size()) {
                next = null;
                return;
            }
            BindingHashMap binding = bindings.get(current);
            current++;
            StarString s = new StarString(star.getSubject(), star.getTriples());

            Iterator<String> vars = binding.getKeyIterator();
            while (vars.hasNext()) {
                String var = vars.next();
                s.updateField(var, binding.get(var).getValue());
            }

            next = s;
        }

        public void reset() {
            current = 0;
        }

        @Override
        public boolean hasNext() {
            if (next == null)
                bufferNext();
            return next != null;
        }

        @Override
        public StarString next() {
            StarString n = next;
            next = null;
            return n;
        }
    }
}
