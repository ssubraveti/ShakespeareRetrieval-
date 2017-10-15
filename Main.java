package com.invertedindex;

import org.json.simple.JSONArray;

import javax.print.attribute.IntegerSyntax;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;


public class Main {

    public static void createIndex(boolean compress) {

        ParseJSON parseJSON = new ParseJSON("src/main/resources/shakespeare-scenes.json");
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
        queryRetriever.loadVocabularyOffsets("src/main/resources/termOffsetMap.txt");
        queryRetriever.loadSceneIdMap("src/main/resources/sceneIdHashtable.txt");
        ArrayList<PriorityQueue<KVPair>> results = new ArrayList<PriorityQueue<KVPair>>();
        try {
            File queryFile = new File(filename);
            PrintWriter resultWriter = new PrintWriter(new BufferedWriter(new FileWriter("src/main/resources/queryResults.txt")));
            FileReader fileReader = new FileReader(queryFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String query = line;
                results.add(queryRetriever.rankDocuments(query, "src/main/resources/invertedIndex.bin"));
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
            queryRetriever.loadVocabularyOffsets("src/main/resources/termOffsetMap.txt");
            queryRetriever.loadSceneIdMap("src/main/resources/sceneIdHashtable.txt");
            PriorityQueue<KVPair> results;

            results = queryRetriever.rankDocuments(query, "src/main/resources/invertedIndex.bin");

            for (int i = 0; i < 5; i++) {
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

    public static void main(String[] args) {
        // write your code here

        /*
        Scanner in = new Scanner(System.in);
        System.out.println("Welcome to the Shakespeare Indexer!");
        System.out.println("What do you want to do?");
        System.out.println("Click:");
        System.out.println("1. To build an uncompressed index");
        System.out.println("2. To build a compressed index");
        System.out.println("3. Use existing index files to test compression hypothesis");
        System.out.println("4. Something else");

        int choice = in.nextInt();

        switch (choice){
            case 1:
                createIndex(false);
                break;
            case 2:
                createIndex(true);
                break;
            case 3:
                long tick = System.nanoTime();
                runQueriesFromFile("src/main/resources/queryFile1.txt",true);
                long tock = System.nanoTime();
                System.out.println("That took: "+(tock-tick)/100000+" ms for file 1");
                 long tick2 = System.nanoTime();
                runQueriesFromFile("src/main/resources/queryFile1.txt",true);
                long tock2 = System.nanoTime();
                System.out.println("That took: "+(tock-tick)/100000+" ms for file 2");


                break;
            case 4:
                break;



        }
        if(choice == 1){
            System.out.println("Would you still like to build the compressed index?");
            System.out.println("    1. Yes");
            System.out.println("    2. No");
            int choice2 = in.nextInt();
            switch (choice2){
                case 1:
                    createIndex(true);
                    break;
                case 2:
                    System.out.println("All required indexes and data structures in place!");
            }
        }
        System.out.println("Do you wish to do some retrieval?");
        System.out.println("    1. Yes");
        System.out.println("    2. No");
        int choice3 = in.nextInt();
        switch (choice3){
            case 1:
                System.out.println("Would you like to use: ");
                System.out.println("    1. Query files");
                System.out.println("    2. Use your own query");
                int choice4 = in.nextInt();
                switch (choice4){
                    case 1:
                        runQueriesFromFile("src/main/resources/queryFile1.txt",false);
                        break;
                    case 2:
                        System.out.println("Enter your query as comma separated strings");
                        String query = in.next();
                        QueryRetriever qr = new QueryRetriever();
                        qr.loadVocabularyOffsets("src/main/resources/termOffsetMap.txt");
                        ArrayList<ArrayList<Posting>> p = qr.fetchTermInvertedLists("cancel","src/main/resources/invertedIndex.bin");
                        ArrayList<Posting> tp = p.get(0);
                        int totalLen = 0;
                        for(Posting posting : tp){
                            totalLen+=2+posting.getPositions().size();
                        }
                        System.out.println(totalLen);
                        //runQueryFromString(query);
                        break;
                        }
                }
        */

        //createIndex(true);
        runQueryFromString("dansker,unshaken,paysan,equinox,imposition,stoccata,essentially", false);


    }


}


