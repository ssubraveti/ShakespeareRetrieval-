package com.invertedindex;


import java.util.*;

public class OrderedWindow implements QueryNode {

    String[] query;
    Integer N;
    int ctf;
    double totalWords;
    QueryRetriever queryRetreiver;

    public OrderedWindow(String[] query, Integer distance) {
        this.query = query;
        this.N = distance;
        queryRetreiver = initQueryRetreiver();
    }

    public QueryRetriever initQueryRetreiver() {

        QueryRetriever queryRetriever = new QueryRetriever();

        queryRetriever.loadSceneIdMap(FilePaths.sceneIdHashMap);
        queryRetriever.loadVocabularyOffsets(FilePaths.termOffsets);
        queryRetriever.loadDocLengthMap(FilePaths.docLengthHashMap);

        return queryRetriever;
    }

    public Double getSmoothedScore(int docid) {

        int doclen = queryRetreiver.getDocLengthMap().get(docid);

        return QueryRetriever.getSmoothingScore(docid, this.totalWords, doclen, 0, this.ctf, 2000);
    }

    public Map<Integer, Double> evaluate() {

        Map<Integer, Double> docscores;
        Double totalWords = 0.0;
        HashMap<Integer, Integer> doclenMap = queryRetreiver.getDocLengthMap();
        //Find out the total words
        for (Integer docid : doclenMap.keySet())
            totalWords += doclenMap.get(docid);

        //Call document at a time for each query
        docscores = performOrderedWindow(this.query, this.N, doclenMap.size(),
                totalWords);

        return docscores;
    }

    public Map<Integer, Double> performOrderedWindow(String[] queryTerms, int windowSize,
                                                     int numDocs, Double totalWords) {
        String fullQuery = "";

        for (String term : queryTerms) {
            fullQuery += term + " ";
        }
        Map<Integer, Double> docscores = new HashMap<Integer, Double>();
        Map<Integer, Integer> doc_windowTf = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> docLenMap = queryRetreiver.getDocLengthMap();
        ArrayList<ArrayList<Posting>> invertedLists = queryRetreiver.fetchTermInvertedLists(fullQuery, FilePaths.indexFile);
        int windowCtf = 0, windowTf, n = invertedLists.size();
        Double doc_score, doc_len;

        for (int d = 1; d <= numDocs; ++d) {


            windowTf = orderedWindowCount(invertedLists, d, windowSize);
            if (n == 2 && queryTerms[0].equals("alas") && queryTerms[1].equals("poor")) {
                System.out.println(d + "->" + windowTf);
            }
            if (windowTf > 0) {
                doc_windowTf.put(d, windowTf);
                windowCtf += windowTf;
            }
        }

        for (int d : doc_windowTf.keySet()) {
            windowTf = doc_windowTf.get(d);
            doc_len = (double) docLenMap.get(d);
            doc_score = QueryRetriever.getSmoothingScore(d, totalWords, doc_len, windowTf, windowCtf, 2000);

            docscores.put(d, doc_score);
        }

        this.ctf = windowCtf;
        return docscores;
    }

    public PriorityQueue<KVPair> rankDocumentsOW(String[] query, int windowSize) {


        int numDocs = queryRetreiver.getDocLengthMap().size();
        this.totalWords = 0.0;

        //Find out the average document length
        for (Integer docid : queryRetreiver.getDocLengthMap().keySet())
            this.totalWords += queryRetreiver.getDocLengthMap().get(docid);

        Map<Integer, Double> docscores = performOrderedWindow(query, windowSize, numDocs, this.totalWords);

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
                if (p.getDocid() == docid)
                    count++;
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

    private HashSet<Integer> getWindowPositions(int s, int endElement, Map<Integer, Set<Integer>> numbers,
                                                int windowSize, int maxPos, HashSet<Integer> pos, int prev) {
        if (prev == endElement) {
            pos.add(s);
            return pos;
        }

        int next_end = Math.min(s + windowSize + 1, maxPos + 1), curr_size;
        HashSet<Integer> tempRes, res, old;
        old = new HashSet<Integer>(pos);
        tempRes = new HashSet<Integer>(pos);
        tempRes.add(s);
        curr_size = tempRes.size();

        for (int j = s + 1; j < next_end; ++j) {
            if (numbers.containsKey(j)) {
                //If we find the next number among all in the set
                for (int marker : numbers.get(j)) {
                    if (marker == prev + 1) {
                        //Check if we can find the next one from here and so on
                        res = getWindowPositions(j, endElement, numbers, windowSize, maxPos, tempRes, marker);
                        if (res.size() > curr_size) return res;
                    }
                }
            }
        }

        //If no possible matches, then return the same set back
        return old;
    }

    private int orderedWindowCount(ArrayList<ArrayList<Posting>> invertedLists, int docid, int windowSize) {

        int querySize = invertedLists.size(), maxPos = -1, n, i, res = 0;

        if (!containsDoc(invertedLists, docid))
            return res;
        ArrayList<ArrayList<Integer>> pos_lists = new ArrayList<ArrayList<Integer>>();

        for (i = 0; i < querySize; ++i)
            pos_lists.add(getPositions(invertedLists.get(i), docid));

        for (ArrayList<Integer> p : pos_lists)
            for (int pos : p)
                maxPos = Math.max(maxPos, pos);

        Map<Integer, Set<Integer>> mask = new HashMap<Integer, Set<Integer>>();

        for (i = 1; i <= maxPos; ++i) {
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

        HashSet<Integer> pos;
        for (i = 1; i <= maxPos; ++i) {
            //If a zero is found, then search for the next n-1 cells recursively to see if subsequent words occur
            if (mask.containsKey(i)) {
                //Without double dipping
                for (int marker : mask.get(i)) {
                    if (marker == 0) {
                        pos = getWindowPositions(i, n - 1, mask, windowSize, maxPos, new HashSet<Integer>(), marker);
                        if (pos.size() == n) {
                            for (Integer pi : pos) mask.remove(pi);
                            ++res;
                        }
                        break;
                    }
                }
            }
        }
        return res;
    }
}


