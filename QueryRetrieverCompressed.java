package com.invertedindex;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class QueryRetrieverCompressed {
    private Map<Integer, String> sceneIdMap;
    private Map<String, LookupData> vocabularyOffsets;

    QueryRetrieverCompressed() {
        sceneIdMap = new HashMap<Integer, String>();
        vocabularyOffsets = new HashMap<String, LookupData>();
    }

    void loadVocabularyOffsets(String filename) {
        try {
            File offsetFile = new File(filename);
            FileReader fileReader = new FileReader(offsetFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] allEntries = line.split(",");
                String term = allEntries[0];
                Long offset = Long.parseLong(allEntries[1]);
                Long length = Long.parseLong(allEntries[2]);
                Long ctf = Long.parseLong(allEntries[3]);
                Long df = Long.parseLong(allEntries[4]);
                LookupData termMetadata = new LookupData(offset, length, ctf, df);
                vocabularyOffsets.put(term, termMetadata);
            }

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }

    }

    void loadSceneIdMap(String filename) {

        try {
            File sceneIdFile = new File(filename);
            FileReader fileReader = new FileReader(sceneIdFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] entries = line.split(",");
                int sceneId = Integer.parseInt(entries[0]);
                String scene = entries[1];
                sceneIdMap.put(sceneId, scene);
            }
        } catch (FileNotFoundException fe) {
            System.out.println("File not found!!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!!");
            ioe.printStackTrace();
        }

    }

    ArrayList<Posting> readFile(Long begin, Long length, String filename) {

        ArrayList<Posting> allPostings = new ArrayList<Posting>();
        try {
            //byte [] postingArray = new byte[length.intValue()];
            ByteBuffer postingArray = ByteBuffer.allocate(length.intValue());
            RandomAccessFile raf = new RandomAccessFile(filename, "r");
            FileChannel fc = raf.getChannel();
            raf.seek(begin);
            fc.read(postingArray);

            postingArray.flip();
            postingArray.clear();
            byte[] byteArray = new byte[postingArray.capacity()];
            postingArray.get(byteArray);
            Decompressor decompressor = new Decompressor();
            Map<Integer, Integer[]> vByteDecodedList = decompressor.vbyteDecode(byteArray);
            int size = 0;

            ArrayList<Integer> deltaDecodedList = new ArrayList<Integer>();
            for (Integer key : vByteDecodedList.keySet()) {
                size = key;
                Integer[] entries = vByteDecodedList.get(key);

                System.out.println("key:" + key);
                for (int i = 0; i < key; i++) {
                    System.out.print(entries[i] + " ");
                }
                System.out.println();
                deltaDecodedList = decompressor.deltaDecode(vByteDecodedList.get(key), key);
            }
            int i = 0;
            int j;
            while (i < deltaDecodedList.size()) {
                int docid = deltaDecodedList.get(i);
                int tf = deltaDecodedList.get(++i);
                ArrayList<Integer> positions = new ArrayList<Integer>();
                for (j = i; j < i + 1 + tf && j < size; j++) {
                    positions.add(deltaDecodedList.get(j));
                }
                i = j;
                allPostings.add(new Posting(docid, tf, positions));

            }

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }
        return allPostings;

    }

    ArrayList<ArrayList<Posting>> fetchTermInvertedLists(String query, String filename) {
        String[] queryTerms = query.split(",");

        ArrayList<ArrayList<Posting>> allTermPostings = new ArrayList<ArrayList<Posting>>();

        for (String term : queryTerms) {
            LookupData termMetadata = vocabularyOffsets.get(term);
            long begin = termMetadata.getOffset();
            long length = termMetadata.getLength();
            ArrayList<Posting> allPostings = readFile(begin, length, filename);
            allTermPostings.add(allPostings);
        }
        return allTermPostings;
    }

    PriorityQueue<KVPair> rankDocuments(String query, String invertedIndexFile) {

        PriorityQueue<KVPair> scores = new PriorityQueue<KVPair>();
        ArrayList<ArrayList<Posting>> termPostings = fetchTermInvertedLists(query, invertedIndexFile);
        int num_documents = sceneIdMap.size();

        for (int docid = 0; docid < num_documents; docid++) {
            calculateScore(docid, termPostings);
            KVPair docIdScorePair = new KVPair(docid, calculateScore(docid, termPostings));
            scores.add(docIdScorePair);
        }
        /*
        for(int i = 0; i<5; i++){
            KVPair top = scores.poll();
            System.out.println(sceneIdMap.get(top.getDocid())+" " + top.getScore());
        }*/
        return scores;

    }

    int calculateScore(int docid, ArrayList<ArrayList<Posting>> termPostings) {
        int score = 0;
        for (ArrayList<Posting> postings : termPostings) {
            for (Posting posting : postings) {
                if (posting.getDocid() == docid) {
                    score += posting.getTf();
                }
            }

        }
        return score;
    }

    Map<String, LookupData> getVocabularyOffsets() {
        return vocabularyOffsets;
    }

    Map<Integer, String> getSceneIdMap() {
        return sceneIdMap;
    }
}
