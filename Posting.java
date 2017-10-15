package com.invertedindex;

import java.util.ArrayList;

public class Posting {

    private int docid;
    private int tf;
    private ArrayList<Integer> positions;

    Posting(int docid, int tf, ArrayList<Integer> positions) {
        this.docid = docid;
        this.tf = tf;
        this.positions = positions;
    }

    int getDocid() {
        return this.docid;
    }

    int getTf() {
        return this.tf;
    }

    ArrayList<Integer> getPositions() {
        return this.positions;
    }

    int sizeOfPosting() {
        return (tf * 4 + 12);
    }

    int lengthOfPosting() {
        return (tf * 2 + 2);
    }
}
