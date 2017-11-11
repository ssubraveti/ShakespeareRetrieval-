/**
 *
 */
package com.invertedindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;


public class UnorderedWindow implements QueryNode {

    String[] query;
    Integer windowSize;
    int ctf;
    double totalWords;
    QueryRetriever queryRetriever;

    public UnorderedWindow(String[] query, Integer windowSize) {
        this.query = query;
        this.windowSize = windowSize;
        queryRetriever = initQueryRetreiver();
    }


    public QueryRetriever initQueryRetreiver() {

        QueryRetriever queryRetriever = new QueryRetriever();

        queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
        queryRetriever.loadVocabularyOffsets(FilePaths.termOffsets);
        queryRetriever.loadDocLengthMap(FilePaths.docLengthHashMap);

        return queryRetriever;
    }

    public Double getSmoothedScore(int docid) {

        int doclen = queryRetriever.getDocLengthMap().get(docid);
        return QueryRetriever.getSmoothingScore(docid, this.totalWords, doclen, 0, this.ctf, 2000);
    }

    public Map<Integer, Double> evaluate() {
        Map<Integer, Double> docscores;

        HashMap<Integer, Integer> doclenMap = queryRetriever.getDocLengthMap();
        this.totalWords = 0.0;

        //Find out the average document length
        for (Integer docid : doclenMap.keySet())
            this.totalWords += doclenMap.get(docid);

        //Call document at a time for each query
        docscores = performUnorderedWindow(this.query, this.windowSize, doclenMap.size(),
                this.totalWords);

        return docscores;
    }

    public Map<Integer, Double> performUnorderedWindow(String[] query, int windowSize,
                                                       int numDocs, Double totalWords) {


        String fullQuery = "";

        for (String term : query) {
            fullQuery += term + " ";
        }
        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<Integer, Integer> doc_windowTf = new HashMap<Integer, Integer>();
        ArrayList<ArrayList<Posting>> inv_lists = queryRetriever.fetchTermInvertedLists(fullQuery, FilePaths.indexFile);
        int windowCtf = 0, windowTf;
        Double doc_score, doc_len;
        HashMap<Integer, Integer> docLengthMap = queryRetriever.getDocLengthMap();

        PriorityQueue<KVPair> pq = new PriorityQueue<KVPair>();

//		string_print("Performing query " + q_num); pn();
        for (int d = 1; d <= numDocs; ++d) {
//			string_print("Checking for " + d); pn();
            windowTf = unorderedWindowCount(inv_lists, d, windowSize);
            if (windowTf > 0) {
                doc_windowTf.put(d, windowTf);
                windowCtf += windowTf;
            }
        }

        for (int d : doc_windowTf.keySet()) {
            windowTf = doc_windowTf.get(d);
            doc_len = (double) docLengthMap.get(d);
            doc_score = QueryRetriever.getSmoothingScore(d, totalWords, doc_len, windowTf, windowCtf, 2000);

            docscores.put(d, doc_score);
            pq.offer(new KVPair(d, doc_score));
        }

        this.ctf = windowCtf;
        return docscores;
    }

    public PriorityQueue<KVPair> rankDocumentsUW(String[] query, int windowSize) {


        int numDocs = queryRetriever.getDocLengthMap().size();
        this.totalWords = 0.0;

        //Find out the average document length
        for (Integer docid : queryRetriever.getDocLengthMap().keySet())
            this.totalWords += queryRetriever.getDocLengthMap().get(docid);

        Map<Integer, Double> docscores = performUnorderedWindow(query, windowSize, numDocs, this.totalWords);

        PriorityQueue<KVPair> ranks = new PriorityQueue<KVPair>();
        for (Map.Entry<Integer, Double> entry : docscores.entrySet()) {
            int docid = entry.getKey();
            double score = entry.getValue();
            KVPair pair = new KVPair(docid, score);
            ranks.add(pair);
        }
        return ranks;
    }

    private boolean containsDoc(ArrayList<ArrayList<Posting>> invertedLists, int docid) {
        int querySize = invertedLists.size();
        int count = 0;
        //All terms should exist for an ordered window to exist
        for (ArrayList<Posting> postings : invertedLists) {
            for (Posting p : postings) {
                if (p.getDocid() == docid) {
                    count++;

                }

            }
        }
        return (count == querySize);
    }

    private ArrayList<Integer> getPositions(ArrayList<Posting> invertedList, int docid) {
        ArrayList<Integer> positions = new ArrayList<Integer>();

        for (Posting posting : invertedList) {
            if (posting.getDocid() == docid) {
                positions = posting.getPositions();
            }
        }
        return positions;
    }


    private int unorderedWindowCount(ArrayList<ArrayList<Posting>> termInvertedLists, int docid, int windowSize) {
        //If query size is more than window size, all terms of the query cannot occur in the doc,
        //and if the docid isn't there in any of the inverted lists
        if (termInvertedLists.size() > windowSize || !containsDoc(termInvertedLists, docid)) return 0;

        int q_size = termInvertedLists.size(), max_pos = -1, n, i, j, res = 0;

        ArrayList<ArrayList<Integer>> pos_lists = new ArrayList<ArrayList<Integer>>();

        //Positions
        for (i = 0; i < q_size; ++i)
            pos_lists.add(getPositions(termInvertedLists.get(i), docid));

        //Maximum position of any term in the query in the document
        for (ArrayList<Integer> p : pos_lists)
            for (int pos : p)
                max_pos = Math.max(max_pos, pos);

        Map<Integer, Set<Integer>> mask = new HashMap<Integer, Set<Integer>>();

        for (i = 1; i <= max_pos; ++i) {
            Set<Integer> pos = new HashSet<Integer>();
            mask.put(i, pos);
        }

        n = pos_lists.size();
        for (i = 0; i < n; ++i) {
            ArrayList<Integer> p = pos_lists.get(i);

            for (int pos : p) {
                mask.get(pos).add(i);
            }
        }
        //Without double dipping
        Map<Integer, Integer> done = new HashMap<Integer, Integer>();
        Set<Integer> unique = new HashSet<Integer>();
        for (i = 0; i <= max_pos; ++i) {
            done.clear();
            unique.clear();

            //Check window for word occurrences
            for (j = i; j < Math.min(max_pos + 1, i + windowSize); ++j) {
                if (mask.containsKey(j)) {
                    for (int marker : mask.get(j)) {
                        if (!done.containsKey(marker)) {
                            unique.add(j);
                            done.put(marker, 1);
                            break;
                        }
                    }
                }
            }
            if (unique.size() == n) {
                ++res;
                for (int pos : unique) {
                    mask.remove(pos);
                }
            }
        }
        return res;
    }
}

