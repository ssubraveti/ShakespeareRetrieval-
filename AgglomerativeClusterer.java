package com.invertedindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AgglomerativeClusterer {

    private QueryRetriever retriever;

    private HashMap<Integer, HashMap<String, Double>> documentVectors;

    AgglomerativeClusterer() {

        retriever = new QueryRetriever();
        retriever.constructDocVectors();
        retriever.writeDocVectorsToFile();
        documentVectors = new HashMap<Integer, HashMap<String, Double>>();
    }

    void readDocVectorsFromFile(String filename) {

        File vectorFile = new File(filename);
        try {
            FileReader fileReader = new FileReader(vectorFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                HashMap<String, Double> components = new HashMap<String, Double>();
                Integer docid = Integer.parseInt(line.split("\\s+")[0]);
                String[] terms = line.split("\\s+")[1].split(",");
                for (String term : terms) {
                    String t = term.split(":")[0];
                    Double score = Double.parseDouble(term.split(":")[1]);
                    components.put(t, score);
                }
                documentVectors.put(docid, components);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    public HashMap<Integer, ArrayList<Integer>> clusterDocsAgglomerative(String linkageType, Double threshold) {


        readDocVectorsFromFile(FilePaths.docVectors);

        HashMap<Integer, ArrayList<Integer>> assignments = new HashMap<Integer, ArrayList<Integer>>();
        ArrayList<Integer> clusterDocs;
        HashMap<String, Double> docvec;

        int num_docs = documentVectors.size(), closest_cluster_id, next_cluster_id = 1;
        Double closest_cost, cost;

        for (int docid = 1; docid <= num_docs; ++docid) {
            closest_cost = Double.MAX_VALUE;
            closest_cluster_id = -1;

            //Get the document vector

            //Try to find the distance with each cluster in assignments
            for (Integer c : assignments.keySet()) {
                clusterDocs = assignments.get(c);
                cost = computeCostFor(docid, clusterDocs, linkageType);
                if (cost < closest_cost && cost < threshold) {
                    closest_cost = cost;
                    closest_cluster_id = c;
                }
            }

            //If no cluster is closest, make a new one by itself
            if (closest_cluster_id == -1) {
                clusterDocs = new ArrayList<Integer>();
                clusterDocs.add(docid);
                assignments.put(next_cluster_id, clusterDocs);
                ++next_cluster_id;
            }

            //Add to the closest cluster
            else {
//				string_print("Going into cluster " + closest_cluster_id); pn(); pn();
                assignments.get(closest_cluster_id).add(docid);
            }

        }

        return assignments;
    }


    private Double computeCostFor(int docid, ArrayList<Integer> clusterDocs, String linkageType) {
        Double cost;
        if (linkageType.equals("min")) {
            cost = Double.MAX_VALUE;
            for (Integer d : clusterDocs) {
                cost = Math.min(cost, 1.0 - computeCosineDistance(documentVectors.get(docid), documentVectors.get(d)));
            }
            return cost;
        } else if (linkageType.equals("max")) {
            cost = Double.MIN_VALUE;
            for (Integer d : clusterDocs) {
                cost = Math.max(cost, 1.0 - computeCosineDistance(documentVectors.get(docid), documentVectors.get(d)));
            }
            return cost;
        } else if (linkageType.equals("avg")) {
            Double numerator = 0.0, denominator = 0.0;
            HashMap<String, Double> temp;
            int n = clusterDocs.size();

            for (Integer d : clusterDocs) {
                temp = documentVectors.get(d);
                numerator += 1.0 - computeCosineDistance(documentVectors.get(docid), temp);
            }

            //|Ci||Cj|
            denominator = 1.0 * (double) n;

            cost = numerator / denominator;
            return cost;
        }

        //Average group linkage
        else if (linkageType.equals("mean")) {
            HashMap<String, Double> muj = new HashMap<String, Double>();
            HashMap<String, Double> temp;
            int n = clusterDocs.size();

            for (Integer d : clusterDocs) {
                temp = documentVectors.get(d);

                //Summation
                for (String term : temp.keySet()) {
                    if (!muj.containsKey(term))
                        muj.put(term, temp.get(term));
                    else
                        muj.put(term, muj.get(term) + temp.get(term));
                }
            }

            //Take mean
            for (String term : muj.keySet()) {
                muj.put(term, muj.get(term) / (double) n);
            }

            cost = 1 - computeCosineDistance(documentVectors.get(docid), muj);
            return cost;
        }

        return -1.0;
    }

    public static Double computeCosineDistance(Map<String, Double> document, Map<String, Double> query) {
        Double similarity = 0.0, dscores = 0.0, qscores = 0.0, q, d;

        for (String word : query.keySet()) {
            q = query.get(word);
            d = 0.0;

            if (document.containsKey(word)) {
                d = document.get(word);
            }
            similarity += q * d;
            qscores += q * q;
        }

        for (String word : document.keySet()) {
            dscores += document.get(word) * document.get(word);
        }

        if (similarity > 0)
            return similarity / Math.sqrt(dscores * qscores);
        return 0.0;
    }


    public HashMap<Integer, HashMap<String, Double>> getDocumentVectors() {
        return documentVectors;
    }
}
