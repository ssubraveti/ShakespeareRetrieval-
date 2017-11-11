package com.invertedindex;
/**
 *
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class FilterRequire implements QueryNode {

    ArrayList<QueryNode> children;
    QueryRetriever queryRetreiver;

    public FilterRequire(ArrayList<QueryNode> childNodes) {
        this.children = childNodes;
        this.queryRetreiver = initQueryRetreiver();
    }

    public QueryRetriever initQueryRetreiver() {

        QueryRetriever queryRetriever = new QueryRetriever();

        queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
        queryRetriever.loadVocabularyOffsets(FilePaths.termOffsets);
        queryRetriever.loadDocLengthMap(FilePaths.docLengthHashMap);

        return queryRetriever;
    }

    public Map<Integer, Double> evaluate() {

        if (this.children.size() != 2) {
            System.out.println("Invalid number of children in filter require: " + this.children.size());
            System.exit(1);
        }

        Map<Integer, Double> proximityExpression = this.children.get(0).evaluate();
        Map<Integer, Double> anyQuery = this.children.get(1).evaluate();

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();

        //Return a hashmap that has the required docs
        for (int d : proximityExpression.keySet()) {
            if (anyQuery.containsKey(d) && anyQuery.get(d) != 0)
                docscores.put(d, anyQuery.get(d));
        }

        return docscores;
    }


    public Double getSmoothedScore(int docid) {
        // TODO Auto-generated method stub
        return null;
    }
}

