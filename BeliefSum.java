package com.invertedindex;

/**
 * This class performs belief sum
 */

import java.util.*;


public class BeliefSum implements QueryNode {

    ArrayList<QueryNode> children;
    QueryRetriever queryRetriever;

    public BeliefSum(ArrayList<QueryNode> childNodes) {
        // TODO Auto-generated constructor stub
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
            docScore += Math.pow(10, this.children.get(i).getSmoothedScore(docid));
        }
        return docScore / this.children.size();
    }

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<Integer, Double> childResult;
        ArrayList<Map<Integer, Double>> childResults = new ArrayList<Map<Integer, Double>>();
        Set<Integer> commonDocids;
        Double docScore, n = (double) this.children.size();

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
                if (childResult.containsKey(docid)) docScore += Math.pow(10, childResult.get(docid));
                else docScore += Math.pow(10, this.children.get(i).getSmoothedScore(docid));
            }
            docScore /= n;
            docscores.put(docid, docScore);
        }

        return docscores;
    }
}

