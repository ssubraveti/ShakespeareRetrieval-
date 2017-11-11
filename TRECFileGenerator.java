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
            PrintWriter printWriter = new PrintWriter(new FileWriter(FilePaths.trecFileBM25));

            for (int i = 0; i < queries.size(); ++i) {
                PriorityQueue<KVPair> results = retriever.rankDocumentsBM25(queries.get(i), FilePaths.indexFile, k1, k2, b);
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
            PrintWriter printWriter = new PrintWriter(new FileWriter(FilePaths.trecFileJM));

            for (int i = 0; i < queries.size(); ++i) {
                PriorityQueue<KVPair> results = retriever.rankDocumentsJMSmoothing(queries.get(i), FilePaths.indexFile, lambda);
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
            PrintWriter printWriter = new PrintWriter(new FileWriter(FilePaths.trecFileDirichlet));

            for (int i = 0; i < queries.size(); ++i) {
                PriorityQueue<KVPair> results = retriever.rankDocumentsDirichletSmoothing(queries.get(i), FilePaths.indexFile, mu);
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

    void buildTRECFileUW(double mu, HashMap<Integer, String> sceneIdMap) {

        System.out.println(mu);


        String modelString = "ssubraveti-uw-dir-2000";

        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter(FilePaths.trecFileUW));

            for (int i = 0; i < queries.size(); ++i) {

                String[] queryTerms = queries.get(i).split("\\s+");
                int windowSize = queryTerms.length;
                UnorderedWindow unorderedWindow = new UnorderedWindow(queryTerms, windowSize);
                PriorityQueue<KVPair> results = unorderedWindow.rankDocumentsUW(queryTerms, windowSize);
                if (results.size() > 0) {
                    int rank = 1;
                    while (!(results.isEmpty())) {
                        KVPair result = results.poll();
                        int docid = result.getDocid();
                        double score = result.getScore();
                        printWriter.println("Q" + (i + 1) + " " + "skip" + " " + sceneIdMap.get(docid) + " " + rank + " " + score + " " + modelString);
                        ++rank;
                    }
                }

            }
            printWriter.close();
        } catch (IOException ioe) {
            System.out.println("I/O Exception!!");
            ioe.printStackTrace();
        }
    }

    void buildTRECFileOW(double mu, HashMap<Integer, String> sceneIdMap) {

        System.out.println(mu);

        String modelString = "ssubraveti-ow-dir-2000";

        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter(FilePaths.trecFileOW));

            for (int i = 0; i < queries.size(); ++i) {

                String[] queryTerms = queries.get(i).split("\\s+");
                int windowSize = 1;
                OrderedWindow unorderedWindow = new OrderedWindow(queryTerms, windowSize);
                PriorityQueue<KVPair> results = unorderedWindow.rankDocumentsOW(queryTerms, windowSize);
                if (results.size() > 0) {
                    int rank = 1;
                    while (!(results.isEmpty())) {
                        KVPair result = results.poll();
                        int docid = result.getDocid();
                        double score = result.getScore();
                        printWriter.println("Q" + (i + 1) + " " + "skip" + " " + sceneIdMap.get(docid) + " " + rank + " " + score + " " + modelString);
                        ++rank;
                    }
                }

            }
            printWriter.close();
        } catch (IOException ioe) {
            System.out.println("I/O Exception!!");
            ioe.printStackTrace();
        }
    }


}
