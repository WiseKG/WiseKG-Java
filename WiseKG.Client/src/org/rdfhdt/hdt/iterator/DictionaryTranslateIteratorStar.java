package org.rdfhdt.hdt.iterator;

import org.wisekg.model.BindingHashMap;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.*;
import org.rdfhdt.hdt.triples.impl.CompoundIteratorStarID;
import org.rdfhdt.hdt.util.StarString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DictionaryTranslateIteratorStar implements IteratorStarString {
    /**
     * The iterator of TripleID
     */
    private IteratorStarID currentIterator;
    /**
     * The dictionary
     */
    final StarStringIterator iterator;
    final HDT hdt;
    private StarString next = null;
    private long numResultEstimation = 0;

    public DictionaryTranslateIteratorStar(StarString star, HDT hdt) {
        this(star, new ArrayList<>(), hdt);
    }

    public DictionaryTranslateIteratorStar(StarString star, List<BindingHashMap> bindings, HDT hdt) {
        this.iterator = new StarStringIterator(bindings, star);
        this.hdt = hdt;
        this.currentIterator = new CompoundIteratorStarID(this.hdt, iterator.next());
        estimateNumResults(star, bindings);
    }

    private void estimateNumResults(StarString star, List<BindingHashMap> bindings) {
        StarStringIterator it = new StarStringIterator(bindings, star);
        while(it.hasNext()) {
            numResultEstimation += estimateNumResultsStar(it.next());
        }
    }

    private long estimateNumResultsStar(StarString star) {
        long size = (long)((double)hdt.getTriples().getNumberOfElements() / (double)star.size());
        double nSize = size;
        int s = star.size();
        for(int i = 0; i < s; i++) {
            TripleString t = star.getTriple(i);
            if(!t.getSubject().equals("") || !t.getObject().equals("")) {
                try {
                    IteratorTripleString it = hdt.search(t.getSubject().toString().startsWith("?") ? "" : t.getSubject(),
                            t.getPredicate().toString().startsWith("?") ? "" : t.getPredicate(),
                            t.getObject().toString().startsWith("?") ? "" : t.getObject());
                    double mult = (double)it.estimatedNumResults() / (double)size;
                    nSize = nSize * mult;
                } catch (NotFoundException e) { continue; }
            }
        }

        return (long)nSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        while (next == null) {
            buffer();
            if (next == null) break;
        }

        return next != null;
    }

    private void buffer() {
        if (currentIterator.hasNext()) {
            next = currentIterator.next().toStarString(hdt.getDictionary());
            return;
        }

        if (iterator.hasNext()) {
            currentIterator = new CompoundIteratorStarID(this.hdt, iterator.next());
            buffer();
            return;
        }

        next = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#next()
     */
    @Override
    public StarString next() {
        if(next == null) buffer();
        StarString ret = next;
        next = null;
        return ret;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public long estimatedNumResults() {
        return numResultEstimation;
    }

    @Override
    public ResultEstimationType numResultEstimation() {
        return ResultEstimationType.APPROXIMATE;
    }

    private static class StarStringIterator implements Iterator<StarString> {
        private final List<BindingHashMap> bindings;
        private final StarString star;
        private int current = 0;
        private StarString next = null;

        StarStringIterator(List<BindingHashMap> bindings, StarString star) {
            this.bindings = bindings;
            this.star = star;
        }

        private void bufferNext() {
            if ((this.bindings == null || this.bindings.size() == 0) && this.current == 0) {
                this.next = this.star;
                ++this.current;
            } else if (this.bindings == null) {
                this.next = null;
            } else if (this.current >= this.bindings.size()) {
                this.next = null;
            } else {
                BindingHashMap binding = (BindingHashMap)this.bindings.get(this.current);
                ++this.current;
                StarString s = new StarString(this.star.getSubject(), this.star.getTriples());
                Iterator vars = binding.getKeyIterator();

                while(vars.hasNext()) {
                    String var = (String)vars.next();
                    s.updateField(var, binding.get(var).getValue());
                }

                this.next = s;
            }
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
            if(next == null) bufferNext();
            StarString n = next;
            next = null;
            return n;
        }
    }
}
