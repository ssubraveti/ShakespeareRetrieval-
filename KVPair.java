package com.invertedindex;

public class KVPair implements Comparable<KVPair> {
    private int docid;
    private int score;

    KVPair(int docid, int score) {
        this.docid = docid;
        this.score = score;
    }

    int getDocid() {
        return docid;
    }

    int getScore() {
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
