/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples/impl/BitmapTriplesIterator.java $
 * Revision: $Rev: 191 $
 * Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 *   Alejandro Andres:          fuzzy.alej@gmail.com
 */

package org.rdfhdt.hdt.triples.impl;

import org.rdfhdt.hdt.compact.bitmap.AdjacencyList;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorsTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.ArrayList;

/**
 * @author mario.arias
 *
 */
public class BitmapTriplesIteratorStar implements IteratorsTripleID {

    private final BitmapTriples triples;
    private final TripleID pattern, returnTriple;
    private long patX, patY, patZ;
    private ArrayList<TripleID> patterns;

    private AdjacencyList adjY, adjZ;
    long posY, posZ, minY, minZ, maxY, maxZ,maxX;
    private long nextY, nextZ;
    private long x, y, z;

    private ArrayList<TripleID> nextSolutions = new ArrayList<>();

    public BitmapTriplesIteratorStar(BitmapTriples triples, ArrayList<TripleID> patterns) {
        this.triples = triples;
        this.returnTriple = new TripleID();
        this.pattern = new TripleID();
        this.patterns = patterns;

        newSearch(patterns);
    }

    public void newSearch(ArrayList<TripleID> patterns) {
        this.pattern.assign(patterns.get(0)); //assign the first pattern

        TripleOrderConvert.swapComponentOrder(this.pattern, TripleComponentOrder.SPO, triples.order);
        patX = this.pattern.getSubject();
        patY = this.pattern.getPredicate();
        patZ = this.pattern.getObject();

        adjY = triples.adjY;
        adjZ = triples.adjZ;

        maxX = adjY.countListsX()+1;
        x=1;
        if (patX!=0){
            x = patX;
            maxX = x+1;
        }
        getNext(); //to position and to get the first result
    }

    private void updateOutput() {
        returnTriple.setAll(x, y, z);
        TripleOrderConvert.swapComponentOrder(returnTriple, triples.order, TripleComponentOrder.SPO);
    }



    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#hasNext()
     */
    @Override
    public boolean hasNext() {
        return !nextSolutions.isEmpty();
    }




    public void getNext(){

        while (x < maxX && nextSolutions.isEmpty()){
            nextSolutions.clear();
            for (int i=0;i<patterns.size();i++) {
                TripleID tid = patterns.get(i);
                patX = tid.getSubject();
                patY = tid.getPredicate();
                patZ = tid.getObject();
                minY = adjY.find(x - 1, patY);
                if (minY == -1) {
                    //not found, will try to search next subject
                    nextSolutions.clear();
                    break;
                }
                maxY = minY + 1;
                if (patZ != 0) {
                    // S P O
                    minZ = adjZ.find(minY, patZ);
                    posZ=minZ;
                    if (minZ == -1) {
                        //not found, will try to search next subject
                        nextSolutions.clear();
                        break;
                    } else {
                        maxZ = minZ + 1;
                    }
                } else {
                    // S P ?
                    posZ = adjZ.find(minY);
                    maxZ = adjZ.last(minY) + 1;
                }
                posY = adjZ.findListIndex(posZ);
                //now iterate the solutions of this patter, could be more than one object
                while (posZ < maxZ){
                    TripleID sol = new TripleID();
                    sol.setAll(x,adjY.get(posY),adjZ.get(posZ));
                    nextSolutions.add(sol);
                    posZ++;
                }

            }
            x++; //next subject
        }

    }

    public ArrayList<TripleID> next() {
        ArrayList<TripleID> ret = new ArrayList<>(nextSolutions);
        nextSolutions.clear();

        //find next solutions
        getNext();
        return ret;
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#hasPrevious()
     */
    @Override
    public boolean hasPrevious() {
        return false;//not supported
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#previous()
     */
    @Override
    public TripleID previous() {
        posZ--;

        posY = adjZ.findListIndex(posZ);

        z = adjZ.get(posZ);
        y = adjY.get(posY);
        x = adjY.findListIndex(posY)+1;

        nextY = adjY.last(x - 1) + 1;
        nextZ = adjZ.last(posY) + 1;

        updateOutput();

        return returnTriple;
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#goToStart()
     */
    @Override
    public void goToStart() {
        posZ = minZ;
        posY = adjZ.findListIndex(posZ);

        z = adjZ.get(posZ);
        y = adjY.get(posY);
        x = adjY.findListIndex(posY)+1;

        nextY = adjY.last(x - 1) + 1;
        nextZ = adjZ.last(posY) + 1;
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#estimatedNumResults()
     */
    @Override
    public long estimatedNumResults() {
        return maxZ - minZ;
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#numResultEstimation()
     */
    @Override
    public ResultEstimationType numResultEstimation() {
        if (patX != 0 && patY == 0 && patZ != 0) {
            return ResultEstimationType.UP_TO;
        }
        return ResultEstimationType.EXACT;
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#canGoTo()
     */
    @Override
    public boolean canGoTo() {
        return pattern.isEmpty();
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#goTo(int)
     */
    @Override
    public void goTo(long pos) {
        if (!canGoTo()) {
            throw new IllegalAccessError("Cannot goto on this bitmaptriples pattern");
        }

        if (pos >= adjZ.getNumberOfElements()) {
            throw new ArrayIndexOutOfBoundsException("Cannot goTo beyond last triple");
        }

        posZ = pos;
        posY = adjZ.findListIndex(posZ);

        z = adjZ.get(posZ);
        y = adjY.get(posY);
        x = adjY.findListIndex(posY)+1;

        nextY = adjY.last(x - 1) + 1;
        nextZ = adjZ.last(posY) + 1;
    }

    /*
     * (non-Javadoc)
     *
     * @see hdt.iterator.IteratorTripleID#getOrder()
     */
    @Override
    public TripleComponentOrder getOrder() {
        return triples.order;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
