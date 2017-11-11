package com.invertedindex;


import java.util.*;


public class BooleanAnd implements QueryNode {

    ArrayList<QueryNode> children;
    QueryRetriever queryRetriever;

    public BooleanAnd(ArrayList<QueryNode> childNodes) {
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

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Set<Integer> commonDocids;

        commonDocids = new HashSet<Integer>(this.children.get(0).evaluate().keySet());
        for (int i = 1; i < this.children.size(); ++i) {
            commonDocids.retainAll(this.children.get(i).evaluate().keySet());
        }

        //Return a dummy hashmap with 0 scores
        for (int d : commonDocids)
            docscores.put(d, 0.0);

        return docscores;
    }

    public Double getSmoothedScore(int docid) {

        return null;
    }
}

