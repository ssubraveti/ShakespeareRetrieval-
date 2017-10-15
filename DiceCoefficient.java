package com.invertedindex;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DiceCoefficient {

    Map<Integer, String[]> docIdTextMap;
    ArrayList<String> vocabulary;

    DiceCoefficient() {
        vocabulary = new ArrayList<String>();
        docIdTextMap = new HashMap<Integer, String[]>();
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
                vocabulary.add(term);
            }
            System.out.println("Done loading Dice offsets!");
            fileReader.close();
            // System.out.println(vocabulary.toString());
        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }

    }


    void loadDocIdTextMap(String filename) {
        try {
            File docFile = new File(filename);
            FileReader fileReader = new FileReader(docFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] allEntries = line.split(",");
                Integer docid = Integer.parseInt(allEntries[0]);
                String[] text = allEntries[1].split("\\s+");
                docIdTextMap.put(docid, text);
            }
            System.out.println("Done loading text");
            fileReader.close();

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }


    }

    Map<String, Integer> computeCounts(String word1, String word2, String[] text) {
        int nab = 0, na = 0, nb = 0;
        Map<String, Integer> counts = new HashMap<String, Integer>();
        for (int i = 1; i < text.length; i++) {
            if (word1.equals(text[i - 1])) {
                na += 1;
                if (word2.equals(text[i])) {
                    nb += 1;
                    nab += 1;

                }
            }
            if (word2.equals(text[i - 1])) {
                nb += 1;
                if (word1.equals(text[i])) {
                    na += 1;
                    nab += 1;
                }
            }

        }
        counts.put("na", na);
        counts.put("nb", nb);
        counts.put("nab", nab);
        return counts;
    }

    String getWordWithBestDiceCoeff(String word1) {
        String bestDiceWord = new String();
        double bestDice = 0;

        String word2;
        for (int i = 0; i < vocabulary.size(); i++) {
            double Na = 0, Nb = 0, Nab = 0, dice = 0;
            word2 = vocabulary.get(i);
            if (!(word1.equals(word2))) {
                ArrayList<Map<String, Integer>> counts = new ArrayList<Map<String, Integer>>();
                for (Integer docid : docIdTextMap.keySet()) {
                    String[] text = docIdTextMap.get(docid);
                    counts.add(computeCounts(word1, word2, text));
                    //System.out.println("Processing Document# "+counts.size());
                }
                for (Map<String, Integer> entry : counts) {
                    Na += entry.get("na");
                    Nb += entry.get("nb");
                    Nab += entry.get("nab");

                }

                if (Na == 0 && Nb == 0)
                    dice = 0;
                else {
                    double numerator = Nab;
                    double denominator = Na + Nb;
                    dice = numerator / denominator;
                }
            }
            if (bestDice < dice) {
                bestDice = dice;
                bestDiceWord = word2;
            }
        }
        return bestDiceWord;
    }

}
