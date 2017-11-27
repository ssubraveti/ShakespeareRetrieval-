package com.invertedindex;


import java.util.*;


public class BeliefMax implements QueryNode {

    ArrayList<QueryNode> children;
    QueryRetriever queryRetriever;

    public BeliefMax(ArrayList<QueryNode> childNodes) {
        this.children = childNodes;
        this.queryRetriever = initQueryRetreiver();
    }

    public QueryRetriever initQueryRetreiver() {

        QueryRetriever queryRetriever = new QueryRetriever();

        queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
        queryRetriever.loadVocabularyOffsets(FilePaths.termOffsets);
        queryRetriever.loadDocLengthMap(FilePaths.docLengthHashMap);

        return queryRetriever;
    }

    public Double getSmoothedScore(int docid) {
        Double docScore = Double.MIN_VALUE;
        for (int i = 0; i < this.children.size(); i++) {
            docScore = Math.max(this.children.get(i).getSmoothedScore(docid), docScore);
        }
        return docScore;
    }

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<Integer, Double> childResult;
        ArrayList<Map<Integer, Double>> child_results = new ArrayList<Map<Integer, Double>>();
        Set<Integer> commonDocids;
        Double docScore;

        commonDocids = new HashSet<Integer>();
        for (int i = 0; i < this.children.size(); i++) {
            childResult = this.children.get(i).evaluate();
            child_results.add(childResult);
            commonDocids.addAll(childResult.keySet());
        }

        //Score the documents
        for (int docid : commonDocids) {
            docScore = 0.0;
            for (int i = 0; i < this.children.size(); i++) {
                childResult = child_results.get(i);
                if (childResult.containsKey(docid))
                    docScore = Math.max(childResult.get(docid), docScore);
                else
                    docScore = Math.max(this.children.get(i).getSmoothedScore(docid), docScore);
            }
            docscores.put(docid, docScore);
        }

        return docscores;
    }
}


