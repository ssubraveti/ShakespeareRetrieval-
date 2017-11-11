package com.invertedindex;

import java.util.Map;

public interface QueryNode {

    public Map<Integer, Double> evaluate();

    public Double getSmoothedScore(int docid);

}
