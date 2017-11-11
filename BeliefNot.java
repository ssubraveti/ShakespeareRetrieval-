package com.invertedindex;


import java.util.HashMap;
import java.util.Map;

public class BeliefNot implements QueryNode {

    QueryNode child;
    QueryRetriever queryRetriever;

    public BeliefNot(QueryNode childNode) {
        this.child = childNode;
        this.queryRetriever = initQueryRetreiver();
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
        Double docScore = 0.0;
        docScore = Math.log10((1.0 - Math.pow(10, this.child.getSmoothedScore(docid))));
        return docScore;
    }

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<Integer, Double> childResult;
        Double docScore;

        childResult = this.child.evaluate();

        //Score the documents
        for (int docid : childResult.keySet()) {
            docScore = Math.log10(1.0 - Math.pow(10, childResult.get(docid)));
            docscores.put(docid, docScore);
        }

        return docscores;
    }
}

