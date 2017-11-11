package com.invertedindex;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Term implements QueryNode {

    String term;
    double totalWords;
    QueryRetriever queryRetriever;

    public Term(String term) {
        this.term = term;
        this.totalWords = 0;
        queryRetriever = initQueryRetreiver();
        // TODO Auto-generated constructor stub
    }

    public QueryRetriever initQueryRetreiver() {

        QueryRetriever queryRetriever = new QueryRetriever();

        queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
        queryRetriever.loadVocabularyOffsets(FilePaths.termOffsets);
        queryRetriever.loadDocLengthMap(FilePaths.docLengthHashMap);

        return queryRetriever;
    }

    public Double getSmoothedScore(int docid) {

        int doclen = queryRetriever.getDocLengthMap().get(docid);
        int ctf = queryRetriever.getVocabularyOffsets().get(this.term).getCtf().intValue();
        return QueryRetriever.getSmoothingScore(docid, this.totalWords, (double) doclen,
                0, ctf, 2000);
    }


    public int getTf(int docid, ArrayList<Posting> postings) {
        int tf = 0;

        for (Posting p : postings) {
            if (docid == p.getDocid())
                tf += p.getTf();
        }
        return tf;
    }

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<String, LookupData> vocabOffsets = queryRetriever.getVocabularyOffsets();
        Map<Integer, Integer> doclenMap = queryRetriever.getDocLengthMap();
        ArrayList<ArrayList<Posting>> inv_lists = queryRetriever.fetchTermInvertedLists(this.term, FilePaths.indexFile);
        int tf, ctf = 0, numDocs = doclenMap.size();
        Double docScore, docLen;

        //Find out the total words
        for (Integer docid : doclenMap.keySet())
            this.totalWords += doclenMap.get(docid);

        //Doc at a time
        for (int d = 1; d <= numDocs; ++d) {
            tf = getTf(d, inv_lists.get(0));
            if (tf > 0) {
                int collectionTf = vocabOffsets.get(this.term).getCtf().intValue();
                docLen = (double) doclenMap.get(d);
                docScore = QueryRetriever.getSmoothingScore(d, this.totalWords, docLen, tf, collectionTf, 2000);
                docscores.put(d, docScore);
            }
        }
        return docscores;
    }
}


