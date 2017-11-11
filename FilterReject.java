package com.invertedindex;
/**
 *
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class FilterReject implements QueryNode {

    ArrayList<QueryNode> children;
    QueryRetriever queryRetriever;

    public FilterReject(ArrayList<QueryNode> childNodes) {
        this.children = childNodes;
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

    public Map<Integer, Double> evaluate() {

        if (this.children.size() != 2) {
            System.out.println("Invalid number of children in filter reject: " + this.children.size());
            System.exit(1);
        }

        Map<Integer, Double> proximityExpression = this.children.get(0).evaluate();
        Map<Integer, Double> anyQuery = this.children.get(1).evaluate();

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();

        //Return a hashmap that has the required docs
        for (int d : anyQuery.keySet()) {
            if (!proximityExpression.containsKey(d) || proximityExpression.get(d) == 0)
                docscores.put(d, anyQuery.get(d));
        }

        return docscores;
    }


    public Double getSmoothedScore(int docid) {
        // TODO Auto-generated method stub
        return null;
    }
}

