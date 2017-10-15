package com.invertedindex;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.*;
import java.util.*;

public class TermVectorizer {

    private Map<Integer, String> playIDMap;
    private Map<String, Integer> reversePlayIdMap;
    private Map<String, Integer> reverseSceneIdMap;
    private Map<Integer, String> sceneIDMap;
    private Map<String, LookupData> termMap;
    private Map<String, ArrayList<Posting>> termPostingMap;


    TermVectorizer() {
        playIDMap = new HashMap<Integer, String>();
        sceneIDMap = new HashMap<Integer, String>();
        termMap = new HashMap<String, LookupData>();
        reversePlayIdMap = new HashMap<String, Integer>();
        reverseSceneIdMap = new HashMap<String, Integer>();
        termPostingMap = new HashMap<String, ArrayList<Posting>>();
    }

    void buildAuxiliaryDataStructures(JSONArray documents) {

        int sceneIdKey = 1;
        int playIdKey = 1;
        //String newline = System.getProperty("line.separator");

        try {
            PrintWriter sceneIdWriter = new PrintWriter(new FileWriter("src/main/resources/sceneIdHashTable.txt"));
            PrintWriter playIdWriter = new PrintWriter(new FileWriter("src/main/resources/PlayIdHashTable.txt"));
            PrintWriter docTextWriter = new PrintWriter(new FileWriter("src/main/resources/docTextHashTable.txt"));

            for (int i = 0; i < documents.size(); i++) {
                JSONObject document = (JSONObject) documents.get(i);
                String sceneId = (String) document.get("sceneId");
                String playId = (String) document.get("playId");
                String docText = (String) document.get("text");
                if (!(sceneIDMap.containsValue(sceneId))) {
                    sceneIDMap.put(sceneIdKey, sceneId);
                    reverseSceneIdMap.put(sceneId, sceneIdKey);
                    sceneIdWriter.println(sceneIdKey + "," + sceneId);
                    docTextWriter.println(sceneIdKey + "," + docText);
                    sceneIdKey++;
                }
                if (!(playIDMap.containsValue(playId))) {
                    playIDMap.put(playIdKey, playId);
                    reversePlayIdMap.put(playId, playIdKey);
                    playIdWriter.println(playIdKey + "," + playId);
                    playIdKey++;
                }

            }
            playIdWriter.close();
            sceneIdWriter.close();
        } catch (FileNotFoundException fe) {
            System.out.println("File not found");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error");
            ioe.printStackTrace();
        }
    }

    void buildInvertedIndex(JSONArray documents) {

        System.out.println(documents.size());

        for (int i = 0; i < documents.size(); i++) {
            JSONObject document = (JSONObject) documents.get(i);
            String text = (String) document.get("text");

            String[] words = text.split("\\s+");
            String[] uniqueWords = new HashSet<String>(Arrays.asList(words)).toArray(new String[0]);
            for (String s : uniqueWords) {
                if (!(termPostingMap.containsKey(s))) {
                    ArrayList<Posting> postings = new ArrayList<Posting>();
                    int docId = reverseSceneIdMap.get(document.get("sceneId"));
                    int tf = 0;
                    ArrayList<Integer> positions = new ArrayList<Integer>();

                    for (int j = 0; j < words.length; j++) {
                        if (words[j].equals(s)) {
                            tf++;
                            positions.add(j + 1);
                        }
                    }
                    Posting pi = new Posting(docId, tf, positions);
                    postings.add(pi);
                    termPostingMap.put(s, postings);
                } else {
                    ArrayList<Posting> postings = termPostingMap.get(s);
                    int docid = reverseSceneIdMap.get(document.get("sceneId"));
                    int tf = 0;
                    ArrayList<Integer> positions = new ArrayList<Integer>();
                    for (int k = 0; k < words.length; k++) {
                        if (words[k].equals(s)) {
                            tf++;
                            positions.add(k + 1);
                        }
                    }
                    Posting pi = new Posting(docid, tf, positions);
                    postings.add(pi);
                    termPostingMap.put(s, postings);
                }

            }

        }

    }

    void dumpIndexAndTermHashMap() {

        long startPosition = 0;

        try {
            RandomAccessFile invertedIndexFile = new RandomAccessFile("src/main/resources/invertedIndex.bin", "rw");
            PrintWriter termMapWriter = new PrintWriter(new FileWriter("src/main/resources/termOffsetMap.txt"));

            for (Map.Entry<String, ArrayList<Posting>> entry : termPostingMap.entrySet()) {
                String term = entry.getKey();
                invertedIndexFile.seek(startPosition);
                long ctf = 0;
                long df = 0;
                for (Posting p : entry.getValue()) {
                    invertedIndexFile.writeInt(p.getDocid());
                    invertedIndexFile.writeInt(p.getTf());
                    ctf += p.getTf();
                    for (int position : p.getPositions()) {
                        invertedIndexFile.writeInt(position);
                    }
                    df += 1;

                }
                long currentPosition = invertedIndexFile.getFilePointer();
                termMap.put(term, new LookupData(startPosition, currentPosition - startPosition, ctf, df));
                startPosition = currentPosition;
            }
            invertedIndexFile.close();

            //Write term offset map to disk
            for (Map.Entry<String, LookupData> entry : termMap.entrySet()) {
                String term = entry.getKey();
                LookupData lookupData = entry.getValue();
                termMapWriter.println(term + ',' + lookupData.getOffset() + ',' + lookupData.getLength() + ',' + lookupData.getCtf() + ',' + lookupData.getDf());
            }
            termMapWriter.close();

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }
    }

    Map<Integer, String> getPlayIdMap() {
        return playIDMap;
    }

    Map<Integer, String> getSceneIdMap() {
        return sceneIDMap;
    }

    Map<String, ArrayList<Posting>> getTermPostingMap() {
        return termPostingMap;
    }

    Map<String, LookupData> getTermMap() {
        return termMap;
    }
}
