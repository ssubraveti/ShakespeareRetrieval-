package com.invertedindex;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class TRECFileGenerator {

    ArrayList<String> queries;

    TRECFileGenerator(String queryFile) {
        queries = new ArrayList<String>();
        try {
            File qFile = new File(queryFile);
            FileReader fileReader = new FileReader(qFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                queries.add(line);
            }

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }
    }

    void buildTRECFileBM25(double k1, double k2, double b, HashMap<Integer, String> sceneIdMap) {

        QueryRetriever retriever = new QueryRetriever();

        String modelString = "ssubraveti-bm25-" + k1 + "-" + k2 + "-" + b;

        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter("src/main/resources/bm25.trecrun"));

            for (int i = 0; i < queries.size(); ++i) {
                PriorityQueue<KVPair> results = retriever.rankDocumentsBM25(queries.get(i), "src/main/resources/invertedIndex.bin", k1, k2, b);
                int rank = 1;
                while (!(results.isEmpty())) {
                    KVPair result = results.poll();
                    int docid = result.getDocid();
                    double score = result.getScore();
                    printWriter.println("Q" + (i + 1) + " " + "skip" + " " + sceneIdMap.get(docid) + " " + rank + " " + score + " " + modelString);
                    ++rank;
                }

            }
            printWriter.close();
        } catch (IOException ioe) {
            System.out.println("I/O Exception!!");
            ioe.printStackTrace();
        }
    }

    void buildTRECFileJMSmoothing(double lambda, HashMap<Integer, String> sceneIdMap) {

        QueryRetriever retriever = new QueryRetriever();

        String modelString = "ssubraveti-ql-jm-" + lambda;

        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter("src/main/resources/ql-jm.trecrun"));

            for (int i = 0; i < queries.size(); ++i) {
                PriorityQueue<KVPair> results = retriever.rankDocumentsJMSmoothing(queries.get(i), "src/main/resources/invertedIndex.bin", lambda);
                int rank = 1;
                while (!(results.isEmpty())) {
                    KVPair result = results.poll();
                    int docid = result.getDocid();
                    double score = result.getScore();
                    printWriter.println("Q" + (i + 1) + " " + "skip" + " " + sceneIdMap.get(docid) + " " + rank + " " + score + " " + modelString);
                    ++rank;
                }

            }
            printWriter.close();
        } catch (IOException ioe) {
            System.out.println("I/O Exception!!");
            ioe.printStackTrace();
        }
    }

    void buildTRECFileDirichletSmoothing(double mu, HashMap<Integer, String> sceneIdMap) {

        System.out.println(mu);
        QueryRetriever retriever = new QueryRetriever();

        String modelString = "ssubraveti-ql-dir-" + mu;

        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter("src/main/resources/ql-dir.trecrun"));

            for (int i = 0; i < queries.size(); ++i) {
                PriorityQueue<KVPair> results = retriever.rankDocumentsDirichletSmoothing(queries.get(i), "src/main/resources/invertedIndex.bin", mu);
                int rank = 1;
                while (!(results.isEmpty())) {
                    KVPair result = results.poll();
                    int docid = result.getDocid();
                    double score = result.getScore();
                    printWriter.println("Q" + (i + 1) + " " + "skip" + " " + sceneIdMap.get(docid) + " " + rank + " " + score + " " + modelString);
                    ++rank;
                }

            }
            printWriter.close();
        } catch (IOException ioe) {
            System.out.println("I/O Exception!!");
            ioe.printStackTrace();
        }
    }

}
