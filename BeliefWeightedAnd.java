package com.invertedindex;
/**
 * This class performs belief weighted and
 */

import java.util.*;


public class BeliefWeightedAnd implements QueryNode {

    ArrayList<QueryNode> children;
    ArrayList<Double> weights;
    QueryRetriever queryRetriever;

    public BeliefWeightedAnd(ArrayList<QueryNode> childNodes, ArrayList<Double> childWeights) {
        // TODO Auto-generated constructor stub
        this.children = childNodes;
        this.weights = childWeights;
        queryRetriever = initQueryRetreiver();
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
            docScore += (this.weights.get(i) * this.children.get(i).getSmoothedScore(docid));
        }
        return docScore;
    }

    public Map<Integer, Double> evaluate() {

        if (this.weights.size() != this.children.size()) {
            System.out.println("All weights not provided for the children nodes!");
            System.exit(1);
        }

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
                if (childResult.containsKey(docid)) docScore += (this.weights.get(i) * childResult.get(docid));
                else docScore += (this.weights.get(i) * this.children.get(i).getSmoothedScore(docid));
            }
            docscores.put(docid, docScore);
        }

        return docscores;
    }
}

