package com.invertedindex;

import java.io.*;
import java.util.*;

public class QueryFileGenerator {
    Map<String, LookupData> vocabularyOffsetMap;

    QueryFileGenerator() {
        vocabularyOffsetMap = new HashMap<String, LookupData>();
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
                vocabularyOffsetMap.put(term, termMetadata);

            }
            fileReader.close();
            System.out.println("Done loading vocabulary");

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }

    }

    void buildQueryFiles() {

        try {
            PrintWriter queryFileWriter = new PrintWriter(new BufferedWriter(new FileWriter("src/main/resources/queryFile1.txt")));
            PrintWriter queryFileWriter2 = new PrintWriter(new BufferedWriter(new FileWriter("src/main/resources/queryFile2.txt")));
            ArrayList<String> vocabulary = new ArrayList<String>(vocabularyOffsetMap.keySet());
            //String[] query = new String[7];
            //Random picker = new Random();

            DiceCoefficient diceCoefficient = new DiceCoefficient();
            diceCoefficient.loadVocabularyOffsets("src/main/resources/termOffsetMap.txt");
            diceCoefficient.loadDocIdTextMap("src/main/resources/docTextHashTable.txt");

            int vocabSize = vocabulary.size();
            List<Integer> indices = new ArrayList<Integer>();
            for (int i = 0; i < vocabSize; i++)
                indices.add(i);
            Collections.shuffle(indices);
            int count = 0;
            for (int queryNum = 0; queryNum < 100; queryNum++) {
                for (int queryTerm = 0; queryTerm < 7; queryTerm++) {
                    int index = indices.get(count);
                    count++;
                    String term = vocabulary.get(index);
                    queryFileWriter.print(term + ",");
                    System.out.println(term + " written to query file 1");
                    String nextTerm = diceCoefficient.getWordWithBestDiceCoeff(term);
                    System.out.println("Processing " + count + "/700");
                    System.out.println("Best term for " + term + " is " + nextTerm + " found!!!");
                    queryFileWriter2.print(term + "," + diceCoefficient.getWordWithBestDiceCoeff(term) + ",");
                    System.out.println(term + " and " + nextTerm + " written to query file 2");
                }
                queryFileWriter.println();
                queryFileWriter2.println();
            }
            queryFileWriter.close();
            queryFileWriter2.close();

        } catch (FileNotFoundException fe) {
            System.out.println("File not found!");
            fe.printStackTrace();
        } catch (IOException ioe) {
            System.out.println("I/O error!");
            ioe.printStackTrace();
        }
    }


}
