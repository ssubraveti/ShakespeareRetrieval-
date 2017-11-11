package com.invertedindex;


import java.util.*;


public class BeliefOr implements QueryNode {

    ArrayList<QueryNode> children;
    QueryRetriever queryRetriever;

    public BeliefOr(ArrayList<QueryNode> childNodes) {
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
        Double product = 1.0;

        for (int i = 0; i < this.children.size(); ++i) {
            product *= (1.0 - Math.pow(10, this.children.get(i).getSmoothedScore(docid)));
        }

        docScore = Math.log10(1.0 - product);
        return docScore;
    }

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<Integer, Double> childResult;
        ArrayList<Map<Integer, Double>> childResults = new ArrayList<Map<Integer, Double>>();
        Set<Integer> commonDocids;
        Double docScore, product;

        commonDocids = new HashSet<Integer>();
        for (int i = 0; i < this.children.size(); ++i) {
            childResult = this.children.get(i).evaluate();
            childResults.add(childResult);
            commonDocids.addAll(childResult.keySet());
        }

        //Score the documents
        for (int docid : commonDocids) {
            docScore = 0.0;
            product = 1.0;
            for (int i = 0; i < this.children.size(); ++i) {
                childResult = childResults.get(i);
                if (childResult.containsKey(docid))
                    product *= (1.0 - Math.pow(10, childResult.get(docid)));
                else
                    product *= (1.0 - Math.pow(10, this.children.get(i).getSmoothedScore(docid)));
            }

            docScore = Math.log10(1.0 - product);
            docscores.put(docid, docScore);
        }

        return docscores;
    }
}

