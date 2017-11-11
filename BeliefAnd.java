package com.invertedindex;


import java.util.*;


public class BeliefAnd implements QueryNode {

    ArrayList<QueryNode> children;
    QueryRetriever queryRetriever;

    public BeliefAnd(ArrayList<QueryNode> childNodes) {
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
        Double docScore = 0.0;
        for (int i = 0; i < this.children.size(); ++i) {
            docScore += this.children.get(i).getSmoothedScore(docid);
        }
        return docScore;
    }

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<Integer, Double> childResult;
        ArrayList<Map<Integer, Double>> childResults = new ArrayList<Map<Integer, Double>>();
        Set<Integer> commonDocids;
        Double docScore;

        commonDocids = new HashSet<Integer>();
        for (int i = 0; i < this.children.size(); ++i) {
            childResult = this.children.get(i).evaluate();
            childResults.add(childResult);
            commonDocids.addAll(childResult.keySet());
        }

        //Score the documents
        for (int docid : commonDocids) {
            docScore = 0.0;
            for (int i = 0; i < this.children.size(); ++i) {
                childResult = childResults.get(i);
                if (childResult.containsKey(docid))
                    docScore += childResult.get(docid);
                else
                    docScore += this.children.get(i).getSmoothedScore(docid);
            }

            docscores.put(docid, docScore);
        }

        return docscores;
    }
}

