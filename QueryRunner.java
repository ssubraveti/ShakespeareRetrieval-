package com.invertedindex;

import java.util.ArrayList;
import java.util.Map;

public class QueryRunner {

    public static Map<Integer, Double> runQuery() {
        Map<Integer, Double> docscores = null;
        ArrayList<QueryNode> child_nodes = new ArrayList<QueryNode>();

        // Filter req (the [and ow:1(king queen royalty)])

        QueryNode the = new Term("the");

        QueryNode orderedWindow = new OrderedWindow("king queen royalty".split("\\s+"), 1);
        ArrayList<QueryNode> andChildren = new ArrayList<QueryNode>();
        andChildren.add(orderedWindow);
        QueryNode beliefAnd = new BeliefAnd(andChildren);

        ArrayList<QueryNode> filterReqParameters = new ArrayList<QueryNode>();
        filterReqParameters.add(the);
        filterReqParameters.add(beliefAnd);

        QueryNode filterRequire = new FilterRequire(filterReqParameters);
        docscores = filterRequire.evaluate();
        return docscores;
    }
}
