package relop;

import java.util.LinkedList;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;

public class HashJoin extends Iterator {

    /**
     * Constructs a selection, given the underlying iterator and predicates.
     */
    private Iterator left, right;
    private boolean started;

    private IndexScan outer, inner;
    private int outerJoin, innerJoin;

    // hashtable info
    private Tuple oTuple;
    private SearchKey oKey;
    private int hashIndex;
    private HashTableDup hashTable;
    private Tuple[] iTuples;
    private int iIndex;


    public HashJoin(Iterator left, Iterator right, int i, int j) {
        this.started = false;
        this.left = left;
        this.right = right;
        this.outerJoin = i;
        this.innerJoin = j;
        this.setSchema(Schema.join(left.getSchema(), right.getSchema()));


        //throw new UnsupportedOperationException("Not implemented");
    }

    private void start() {
        started = true;
        // build indexscan on outer
        this.outer = new IndexScan(left, outerJoin);
        // build indexscan on inner
        this.inner = new IndexScan(right, innerJoin);
        startProbing();
    }

    private void startProbing() {
        // start of probing
        this.outer.restart();
        // finds the first outer bucket
        hashIndex = this.outer.getNextHash();
        oTuple = this.outer.getNext();
        oKey = this.outer.getLastKey();
        buildBucket();
    }

    private void buildBucket() {
        // creates the hashTable
        hashTable = new HashTableDup();
        //adds all relevant inner tuples to the hashTable
        if(inner.findBucket(hashIndex)) {
            while(inner.getNextHash() == hashIndex) {
                Tuple iTuple = inner.getNext();
                SearchKey iKey = inner.getLastKey();
                hashTable.add(iKey, iTuple);
            }
        }
        iTuples = hashTable.getAll(oKey);
        iIndex = 0;
    }

    /**
     * Gives a one-line explaination of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
        this.indent(depth);
        System.out.println("HashJoin");
        left.explain(depth +1);
        right.explain(depth +1);
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        outer.restart();
        inner.restart();
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        return outer.isOpen();
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        outer.close();
        inner.close();
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
        if(!started) {
            start();
        }
        // search current table for next match
        while(iTuples != null && iIndex < iTuples.length) {
            //check for equality on fields
            if (oTuple.getField(outerJoin) == iTuples[iIndex].getField(innerJoin)) {
                // next tuple is current index
                return true;
            }
            // else increment to next tuple
            iIndex++;
        }
        // no match, go onto next tuple and repeat search
        // find the next tuple that fits
        while(outer.hasNext()) {
            //get the next tuple
            int nextHashIndex = outer.getNextHash();
            oTuple = outer.getNext();
            oKey = outer.getLastKey();
            //System.out.print("hash: " + nextHashIndex);
            //System.out.println("tuple: " + oTuple.toString());
            // if the hash indexs dont match, rebuild hash table
            if(nextHashIndex != hashIndex) {
                hashIndex = nextHashIndex;
                buildBucket();
            }
            iIndex = 0;
            // see if any tuples match
            while(iTuples != null && iIndex < iTuples.length) {
                //check for equality on fields
                if (oTuple.getField(outerJoin) == iTuples[iIndex].getField(innerJoin)) {
                    // next tuple is current index
                    return true;
                }
                // else increment to next tuple
                iIndex++;
            }
        }
        // no more tuples exist
        return false;
        //throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Gets the next tuple in the iteration.
     *
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        if(hasNext()) {
            // should be oTuple x iTuple[iIndex]
            Tuple ret = Tuple.join(oTuple, iTuples[iIndex], this.getSchema());
            iIndex++;
            return ret;
        }
        throw new IllegalStateException();
    }

}
