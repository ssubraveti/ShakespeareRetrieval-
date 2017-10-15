package com.invertedindex;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

public class TermVectorizerCompressed {
    private Map<Integer, String> playIDMap;
    private Map<String, Integer> reversePlayIdMap;
    private Map<String, Integer> reverseSceneIdMap;
    private Map<Integer, String> sceneIDMap;
    private Map<String, LookupData> termMap;
    private Map<String, ArrayList<Posting>> termPostingMap;
    private Map<String, ArrayList<Integer>> compressedTermPostingMap;

    //Map<String, positionIndex> termMap;

    TermVectorizerCompressed() {
        playIDMap = new HashMap<Integer, String>();
        sceneIDMap = new HashMap<Integer, String>();
        termMap = new HashMap<String, LookupData>();
        reversePlayIdMap = new HashMap<String, Integer>();
        reverseSceneIdMap = new HashMap<String, Integer>();
        termPostingMap = new HashMap<String, ArrayList<Posting>>();
        compressedTermPostingMap = new HashMap<String, ArrayList<Integer>>();
    }

    void buildAuxiliaryDataStructures(JSONArray documents) {

        int sceneIdKey = 1;
        int playIdKey = 1;
        //String newline = System.getProperty("line.separator");

        try {
            PrintWriter sceneIdWriter = new PrintWriter(new BufferedWriter(new FileWriter("src/main/resources/sceneIdHashTable.txt")));
            PrintWriter playIdWriter = new PrintWriter(new BufferedWriter(new FileWriter("src/main/resources/PlayIdHashTable.txt")));

            for (int i = 0; i < documents.size(); i++) {
                JSONObject document = (JSONObject) documents.get(i);
                String sceneId = (String) document.get("sceneId");
                String playId = (String) document.get("playId");
                if (!(sceneIDMap.containsValue(sceneId))) {
                    sceneIDMap.put(sceneIdKey, sceneId);
                    reverseSceneIdMap.put(sceneId, sceneIdKey);
                    sceneIdWriter.println(sceneIdKey + "," + sceneId);
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

    ArrayList<Integer> deltaEncode(ArrayList<Integer> inputSequence) {

        ArrayList<Integer> result = new ArrayList<Integer>();
        int previousDoc = -1;
        int nextDoc;
        int previousPosition;
        int i = 0;
        while (i < inputSequence.size()) {

            if (previousDoc == -1) {
                result.add(inputSequence.get(i));
                previousDoc = inputSequence.get(i);
                i++;
            } else {
                result.add(inputSequence.get(i) - previousDoc);
                previousDoc = inputSequence.get(i);
                i++;
            }
            int tf = inputSequence.get(i);
            result.add(tf);
            previousPosition = -1;
            int j = i + 1;
            while (j < i + 1 + tf) {
                if (previousPosition == -1) {
                    result.add(inputSequence.get(j));
                    previousPosition = inputSequence.get(j);

                } else {
                    result.add(inputSequence.get(j) - previousPosition);
                    previousPosition = inputSequence.get(j);

                }
                j++;

            }
            i = j;

        }
        return result;

    }

    byte[] vbyteEncodeInt(int n) {
        if (n == 0) {
            return new byte[]{0};
        }

        int i = (int) (Math.log(n) / Math.log(128)) + 1;
        byte[] encodedInt = new byte[i];

        int j = i - 1;
        do {
            encodedInt[j--] = (byte) (n % 128);
            n /= 128;
        } while (j >= 0);

        encodedInt[i - 1] += 128;
        return encodedInt;
    }

    byte[] vByteCompress(ArrayList<Integer> data, int size) {
        ByteBuffer buff = ByteBuffer.allocate(size * (Integer.SIZE / Byte.SIZE));

        for (int i = 0; i < size; ++i) {
            buff.put(vbyteEncodeInt(data.get(i)));
        }

        buff.flip();
        byte[] encodedInts = new byte[buff.limit()];
        buff.get(encodedInts);
        return encodedInts;
    }

    Map<Integer, ArrayList<Integer>> vbyteDecode(byte[] bytes) {
        Map<Integer, ArrayList<Integer>> lengthArrayMap = new HashMap<Integer, ArrayList<Integer>>();
        ArrayList<Integer> data = new ArrayList<Integer>();

        int n = 0, invertedListSize = 0;

        for (byte b : bytes) {
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                data.set(invertedListSize++, num);
                n = 0;
            }
        }
        lengthArrayMap.put(invertedListSize, data);
        return lengthArrayMap;
    }

    ArrayList<Integer> deltaDecode(ArrayList<Integer> inputSequence) {
        ArrayList<Integer> result = new ArrayList<Integer>();
        int previousDoc = -1;
        int nextDoc;
        int previousPosition;
        int i = 0;
        while (i < inputSequence.size()) {

            if (previousDoc == -1) {
                result.add(inputSequence.get(i));
                previousDoc = result.get(i);
                i++;
            } else {
                result.add(inputSequence.get(i) + previousDoc);
                previousDoc = result.get(i);
                i++;
            }
            int tf = inputSequence.get(i);
            result.add(tf);
            previousPosition = -1;
            int j = i + 1;
            while (j < i + 1 + tf) {
                if (previousPosition == -1) {
                    result.add(inputSequence.get(j));
                    previousPosition = result.get(j);

                } else {
                    result.add(inputSequence.get(j) + previousPosition);
                    previousPosition = result.get(j);

                }
                j++;

            }
            i = j;

        }
        return result;

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


    void flattenInvertedIndex() {

        for (Map.Entry<String, ArrayList<Posting>> entry : termPostingMap.entrySet()) {
            String term = entry.getKey();
            ArrayList<Integer> flattenedPostings = new ArrayList<Integer>();
            for (Posting p : entry.getValue()) {
                flattenedPostings.add(p.getDocid());
                flattenedPostings.add(p.getTf());
                for (int position : p.getPositions()) {
                    flattenedPostings.add(position);
                }
            }
            // System.out.println("Length: "+ flattenedPostings.size());
            compressedTermPostingMap.put(term, flattenedPostings);
        }

    }

    void dumpIndexAndTermHashMap() {

        flattenInvertedIndex();
        long startPosition = 0;

        try {
            RandomAccessFile invertedIndexFile = new RandomAccessFile("src/main/resources/invertedIndexCompressed.bin", "rw");
            PrintWriter termMapWriter = new PrintWriter(new BufferedWriter(new FileWriter("src/main/resources/termOffsetMapCompressed.txt")));

            for (Map.Entry<String, ArrayList<Integer>> entry : compressedTermPostingMap.entrySet()) {

                String term = entry.getKey();
                int ctf = calculateCtf(term);
                int df = calculateDf(term);
                invertedIndexFile.seek(startPosition);
                ArrayList<Integer> postings = entry.getValue();
                ArrayList<Integer> deltaEncodedPostings = deltaEncode(postings);
                byte[] compressedPostings = vByteCompress(deltaEncodedPostings, postings.size());
                //System.out.println(deltaEncodedPostings.size()==postings.size());

                Decompressor decompressor = new Decompressor();

                int key = (Integer) decompressor.vbyteDecode(compressedPostings).keySet().toArray()[0];

                invertedIndexFile.write(compressedPostings);
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

    int calculateCtf(String term) {

        ArrayList<Posting> postings = termPostingMap.get(term);
        int ctf = 0;
        for (Posting p : postings) {
            ctf += p.getTf();
        }
        return ctf;
    }

    int calculateDf(String term) {
        ArrayList<Posting> postings = termPostingMap.get(term);

        return postings.size();
    }

    Map<String, LookupData> getTermMap() {
        return termMap;
    }


}
