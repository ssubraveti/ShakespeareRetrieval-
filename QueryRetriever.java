package com.invertedindex;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class QueryRetriever {
    private PriorityQueue<KVPair> scores;
    private Map<Integer, String> sceneIdMap;
    private Map<String, LookupData> vocabularyOffsets;
    private HashMap<Integer, HashMap<String, Double>> documentVectors;
    private HashMap<Integer, Double> denominators;
    private HashMap<Integer, Integer> docLengthMap;

    QueryRetriever() {
        scores = new PriorityQueue<KVPair>();
        sceneIdMap = new HashMap<Integer, String>();
        vocabularyOffsets = new HashMap<String, LookupData>();
        documentVectors = new HashMap<Integer, HashMap<String, Double>>();
        denominators = new HashMap<Integer, Double>();
        docLengthMap = new HashMap<Integer, Integer>();
    }

    void loadDocLengthMap(String filename) {
        try {
            File lenFile = new File(filename);
            FileReader fileReader = new FileReader(lenFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] allEntries = line.split(",");
                Integer docid = Integer.parseInt(allEntries[0]);
                Integer length = Integer.parseInt(allEntries[1]);
                docLengthMap.put(docid, length);
            }

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }


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
            while (postingArray.hasRemaining()) {
                int docid = postingArray.getInt();
                int tf = postingArray.getInt();
                ArrayList<Integer> positions = new ArrayList<Integer>();
                for (int i = 0; i < tf; i++) {
                    positions.add(postingArray.getInt());
                }
                allPostings.add(new Posting(docid, tf, positions));

            }
            fc.close();
            raf.close();

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
        String[] queryTerms = query.split("\\s+");

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

    ArrayList<Posting> getTermInvertedList(String term, String filename) {
        LookupData termMetadata = vocabularyOffsets.get(term);
        long begin = termMetadata.getOffset();
        long length = termMetadata.getLength();
        ArrayList<Posting> allPostings = readFile(begin, length, filename);

        return allPostings;
    }
    PriorityQueue<KVPair> rankDocuments(String query, String invertedIndexFile) {


        PriorityQueue<KVPair> scores = new PriorityQueue<KVPair>();

        ArrayList<ArrayList<Posting>> termPostings = fetchTermInvertedLists(query, invertedIndexFile);
        int num_documents = sceneIdMap.size();
        for (int docid = 1; docid <= num_documents; docid++) {
            int docScores = calculateScore(docid, termPostings);
            KVPair docIdScorePair = new KVPair(docid, docScores);
            scores.add(docIdScorePair);
        }
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

    void constructDocVectors() {

        double numDocs = sceneIdMap.size();

        loadVocabularyOffsets("src/main/resources/termOffsetMap.txt");
        loadSceneIdMap("src/main/resources/sceneIdHashTable.txt");
        //HashMap<Integer, HashMap<String,Double>> numerators = new HashMap<Integer, HashMap<String, Double>>();
        //HashMap<Integer, Double> denominators = new HashMap<Integer, Double>();

        for (String term : vocabularyOffsets.keySet()) {
            ArrayList<Posting> termPostings = getTermInvertedList(term, "src/main/resources/invertedIndex.bin");

            for (Posting p : termPostings) {
                //current docid
                int docid = p.getDocid();
                long df = vocabularyOffsets.get(term).getDf();
                int tf = p.getTf();

                //To calculate numerators and denominators for a particular term of a particular document

                HashMap<String, Double> termNumerators = documentVectors.get(docid);
                Double denominator = denominators.get(docid);
                denominator = (denominator == null) ? 0.0 : denominator;
                double numerator = (Math.log10(tf) + 1) * (Math.log10(numDocs / df));
                denominator += Math.pow(numerator, 2);
                denominators.put(docid, denominator);
                if (termNumerators == null) {
                    termNumerators = new HashMap<String, Double>();
                    termNumerators.put(term, numerator);
                } else
                    termNumerators.put(term, numerator);
                documentVectors.put(docid, termNumerators);
                //denominators.put(docid,termDenominators);
            }
        }
        System.out.println("Numerators size: " + documentVectors.size());
        System.out.println("Denominators size: " + denominators.size());


    }

    HashMap<String, Double> constructQueryVector(String query) {

        //loadVocabularyOffsets("src/main/resources/termOffsetMap.txt");

        double numDocs = sceneIdMap.size();

        HashMap<String, Integer> termFrequencies = new HashMap<String, Integer>();
        HashMap<String, Double> termWeights = new HashMap<String, Double>();

        String[] terms = query.split("\\s+");

        for (String term : terms) {
            Integer count = termFrequencies.get(term);
            count = (count == null) ? 1 : ++count;
            termFrequencies.put(term, count);
        }

        double denominator = 0.0;
        for (Map.Entry<String, Integer> entry : termFrequencies.entrySet()) {
            String term = entry.getKey();
            int frequency = entry.getValue();

            double numerator = (Math.log10(frequency + 1) * (Math.log10(numDocs / vocabularyOffsets.get(term).getDf())));
            denominator += Math.pow(numerator, 2);
            termWeights.put(term, numerator);

        }
        for (String term : termWeights.keySet()) {
            double finalTermWeight = termWeights.get(term) / Math.sqrt(denominator);
            termWeights.put(term, finalTermWeight);
        }

        return termWeights;
    }

    double calculateDocScoreVSM(int docid, HashMap<String, Double> queryVector) {

        double score = 0;
        HashMap<String, Double> docVector = documentVectors.get(docid);
        for (String term : queryVector.keySet()) {
            double docWeight = (docVector.get(term) == null) ? 0.0 : docVector.get(term);
            double termWeight = queryVector.get(term);
            score += docWeight * termWeight;
        }

        double normalizedScore = score / Math.sqrt(denominators.get(docid));
        return normalizedScore;
    }

    PriorityQueue<KVPair> rankDocumentsVSM(String query, String invertedIndexFile) {

        HashMap<String, Double> queryVector = constructQueryVector(query);

        HashSet<Integer> docids = new HashSet<Integer>();

        ArrayList<ArrayList<Posting>> termPostings = fetchTermInvertedLists(query, invertedIndexFile);

        for (ArrayList<Posting> postings : termPostings) {
            for (Posting posting : postings) {
                docids.add(posting.getDocid());
            }
        }

        PriorityQueue<KVPair> scores = new PriorityQueue<KVPair>();

        //ArrayList<ArrayList<Posting>> termPostings = fetchTermInvertedLists(query, invertedIndexFile);
        //int num_documents = sceneIdMap.size();
        for (int docid : docids) {
            double docScore = calculateDocScoreVSM(docid, queryVector);
            KVPair docIdScorePair = new KVPair(docid, docScore);
            scores.add(docIdScorePair);
        }
        return scores;
    }

    double calculateDocScoreBM25(int docid, String query) {

        double k1 = 1.2;
        double k2 = 100;
        double b = 0.75;
        String[] queryTerms = query.split("\\s+");
        loadDocLengthMap("src/main/resources/docLengthHashTable.txt");
        double avgLength = 0.0;
        double N = docLengthMap.size();
        for (int documentId : docLengthMap.keySet()) {
            avgLength += docLengthMap.get(documentId);
        }
        avgLength /= N;

        HashMap<Integer, Double> KValues = new HashMap<Integer, Double>();

        for (int documentId : docLengthMap.keySet()) {
            int length = docLengthMap.get(documentId);

            double K = k1 * ((1 - b) + b * (length / avgLength));
            KValues.put(documentId, K);
        }

        HashMap<String, Integer> queryTermFrequencies = new HashMap<String, Integer>();

        for (String term : queryTerms) {
            Integer frequency = queryTermFrequencies.get(term);
            frequency = (frequency == null) ? 1 : ++frequency;

            queryTermFrequencies.put(term, frequency);

        }

        double score = 0.0;
        for (String term : queryTermFrequencies.keySet()) {
            ArrayList<Posting> postings = getTermInvertedList(term, "src/main/resources/invertedIndex.bin");
            for (Posting p : postings) {
                int currentDocID = p.getDocid();
                if (currentDocID == docid) {
                    long ni = vocabularyOffsets.get(term).getDf();
                    int fi = p.getTf();
                    double KValue = KValues.get(docid);
                    int qf = queryTermFrequencies.get(term);
                    score += Math.log((N - ni + 0.5) / (ni + 0.5)) * (((k1 + 1) * fi) / (KValue + fi)) * (((k2 + 1) * qf) / (k2 + qf));
                    break;

                }

            }

        }
        return score;
    }

    PriorityQueue<KVPair> rankDocumentsBM25(String query, String invertedIndexFile) {


        HashSet<Integer> docids = new HashSet<Integer>();

        ArrayList<ArrayList<Posting>> termPostings = fetchTermInvertedLists(query, invertedIndexFile);

        for (ArrayList<Posting> postings : termPostings) {
            for (Posting posting : postings) {
                docids.add(posting.getDocid());
            }
        }

        PriorityQueue<KVPair> scores = new PriorityQueue<KVPair>();

        for (int docid : docids) {
            double docScore = calculateDocScoreBM25(docid, query);
            KVPair docIdScorePair = new KVPair(docid, docScore);
            scores.add(docIdScorePair);
        }
        return scores;
    }


    Map<String, LookupData> getVocabularyOffsets() {
        return vocabularyOffsets;
    }

    Map<Integer, String> getSceneIdMap() {
        return sceneIdMap;
    }

    PriorityQueue<KVPair> getScores() {
        return scores;
    }
}
