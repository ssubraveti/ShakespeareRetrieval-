package com.invertedindex;

public class KVPair implements Comparable<KVPair> {
    private int docid;
    private double score;

    KVPair(int docid, double score) {
        this.docid = docid;
        this.score = score;
    }

    int getDocid() {
        return docid;
    }

    double getScore() {
        return score;
    }


    public int compareTo(KVPair x) {

        if (this.getScore() > x.getScore())
            return -1;
        if (this.getScore() < x.getScore())
            return 1;
        else
            return 0;

    }
}
