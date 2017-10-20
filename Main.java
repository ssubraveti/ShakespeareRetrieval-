package com.invertedindex;

import org.json.simple.JSONArray;

import javax.print.attribute.IntegerSyntax;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


public class Main {

    public static void createIndex(boolean compress) {

        ParseJSON parseJSON = new ParseJSON(FilePaths.scenesFile);
        JSONArray allDocuments = parseJSON.getAllDocuments();

        if (!compress) {
            TermVectorizer termVectorizer = new TermVectorizer();
            termVectorizer.buildAuxiliaryDataStructures(allDocuments);
            termVectorizer.buildInvertedIndex(allDocuments);
            termVectorizer.dumpIndexAndTermHashMap();
        } else {
            TermVectorizerCompressed termVectorizerCompressed = new TermVectorizerCompressed();
            termVectorizerCompressed.buildAuxiliaryDataStructures(allDocuments);
            termVectorizerCompressed.buildInvertedIndex(allDocuments);
            termVectorizerCompressed.dumpIndexAndTermHashMap();
        }
    }

    public static void runQueriesFromFile(String filename, boolean experiment) {

        QueryRetriever queryRetriever = new QueryRetriever();
        queryRetriever.loadVocabularyOffsets(FilePaths.termOffsets);
        queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
        ArrayList<PriorityQueue<KVPair>> results = new ArrayList<PriorityQueue<KVPair>>();
        try {
            File queryFile = new File(filename);
            PrintWriter resultWriter = new PrintWriter(new BufferedWriter(new FileWriter("src/main/resources/queryResults.txt")));
            FileReader fileReader = new FileReader(queryFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String query = line;
                results.add(queryRetriever.rankDocuments(query, FilePaths.indexFile));
            }
            if (!experiment) {
                for (PriorityQueue<KVPair> result : results) {
                    for (int i = 0; i < 5; i++) {
                        String file = queryRetriever.getSceneIdMap().get(result.poll().getDocid());
                        resultWriter.print(file + ",");
                    }
                    resultWriter.println();
                }
                resultWriter.close();
            }
        } catch (FileNotFoundException fe) {
            System.out.println("File not found!!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!!");
            ioe.printStackTrace();
        }


    }

    public static void runQueryFromString(String query, boolean compressed) {

        if (!compressed) {
            QueryRetriever queryRetriever = new QueryRetriever();
            queryRetriever.loadVocabularyOffsets(FilePaths.termOffsets);
            queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
            queryRetriever.constructDocVectors();
            //queryRetriever.constructQueryVector(query);
            PriorityQueue<KVPair> results;
            results = queryRetriever.rankDocumentsVSM(query, FilePaths.indexFile);

            System.out.println("Top 20 documents: ");
            for (int i = 0; i < 20; i++) {
                KVPair answer = results.poll();
                System.out.println("DocId: " + answer.getDocid() + " Score: " + answer.getScore());

            }
        } else {
            QueryRetrieverCompressed queryRetriever = new QueryRetrieverCompressed();
            queryRetriever.loadVocabularyOffsets("src/main/resources/termOffsetMap.txt");
            queryRetriever.loadSceneIdMap("src/main/resources/sceneIdHashtable.txt");
            PriorityQueue<KVPair> results;

            results = queryRetriever.rankDocuments(query, "src/main/resources/invertedIndexCompressed.bin");

            for (int i = 0; i < 5; i++) {
                KVPair answer = results.poll();
                System.out.println("DocId: " + answer.getDocid() + " Score: " + answer.getScore());

            }
        }


    }

    public static void buildTRECFiles() {
        //createIndex(false);

        Scanner in = new Scanner(System.in);
        QueryRetriever queryRetriever = new QueryRetriever();
        queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
        HashMap<Integer, String> sceneIdMap = queryRetriever.getSceneIdMap();
        TRECFileGenerator trecFileGenerator = new TRECFileGenerator(FilePaths.queryFile);

        System.out.println("Enter mu value for ranking documents with the Query Likelihood Model using Dirichlet Smoothing: ");
        double mu = in.nextDouble();
        trecFileGenerator.buildTRECFileDirichletSmoothing(mu, sceneIdMap);
        System.out.println("TREC file ql-dir.trecrun built!!");


        System.out.println("Enter lambda value for ranking documents with the Query Likelihood Model using Jellnick-Mercer Smoothing: ");
        double lambda = in.nextDouble();
        trecFileGenerator.buildTRECFileJMSmoothing(lambda, sceneIdMap);

        System.out.println("TREC file ql-jm.trecrun built!");

        System.out.println("Enter parameter values for ranking using BM25 model: ");
        System.out.println("k1: ");
        double k1 = in.nextDouble();
        System.out.println("k2: ");
        double k2 = in.nextDouble();
        System.out.println("b: ");
        double b = in.nextDouble();
        trecFileGenerator.buildTRECFileBM25(k1, k2, b, sceneIdMap);
        System.out.println("TREC file bm25.trecrun built!!");
    }

    public static void main(String[] args) {
        // write your code here


        Scanner in = new Scanner(System.in);
        System.out.println("Welcome to the Shakespeare Indexer!");
        System.out.println("What do you want to do?");
        System.out.println("Click:");
        System.out.println("1. To build the index");
        System.out.println("2. Use existing index to build TREC files");

        int choice = in.nextInt();

        switch (choice){
            case 1:
                createIndex(false);
                break;
            case 2:
                buildTRECFiles();
                break;



        }
        if(choice == 1){
            System.out.println("Index built! Proceed to building TREC files?");
            System.out.println("    1. Yes");
            System.out.println("    2. No");
            int choice2 = in.nextInt();
            switch (choice2){
                case 1:
                    System.out.println("Building TREC files");
                    buildTRECFiles();
                    break;

                case 2:
                    System.out.println("All required indexes and data structures in place!");
                    break;
            }
        }
        System.out.println("Try out the Vector Space Model?");
        System.out.println("    1. Yes");
        System.out.println("    2. No");
        int choice2 = in.nextInt();
        String buffer = in.nextLine();
        switch (choice2) {
            case 1:
                System.out.print("Enter query as space separated strings: ");
                String query = in.nextLine();
                runQueryFromString(query, false);
                break;
            case 2:
                System.out.println("Quitting!");
                break;
        }


    }

}


