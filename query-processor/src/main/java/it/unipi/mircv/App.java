package it.unipi.mircv;

import it.unipi.mircv.beans.*;
import it.unipi.mircv.parser.Parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class App 
{
    public static void main( String[] args )
    {
        /*DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();
        System.out.println(documentIndex.size());*/

        Lexicon lexicon = new Lexicon();
        lexicon.loadLexicon();
        System.out.println("Lexicon size: " + lexicon.size());

        DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.loadDocumentIndex();
        System.out.println("Document index size: " + documentIndex.size());

        Statistics statistics = new Statistics();

        double k1 = 1.5;
        double b = 0.75;

        /*System.out.println(lexicon.get("ball"));
        System.out.println(lexicon.get("dog"));
        System.out.println(lexicon.get("sport"));

        PostingList postingList = new PostingList();
        postingList.openList(lexicon.get("ball"));

        System.out.println("Ball: " + postingList);

        postingList = new PostingList();
        postingList.openList(lexicon.get("dog"));

        System.out.println("Dog: " + postingList);

        postingList = new PostingList();
        postingList.openList(lexicon.get("sport"));

        System.out.println("Sport" + postingList);*/

        // TODO: 04/04/2023 parse the query
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a query:");
        String query = "-1\t" + scanner.nextLine();

        List<String> stopwords = null;

        // TODO: 04/04/2023 Move this into parser
        //If the stopwords removal and the stemming is requested, the stopwords are read from a file
        try {
            //todo: move stopwords-en.txt outside inverted-index because it is common between inv-index and query-proc
            stopwords = Files.readAllLines(Paths.get("inverted-index/src/main/resources/utility/stopwords-en.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ParsedDocument parsedDocument = Parser.processDocument(query, true, stopwords);
        if(parsedDocument == null){
            System.out.println("No results");
            return;
        }
        String[] query_terms = parsedDocument.getTerms();
        PostingList[] postingArray = new PostingList[query_terms.length];
        HashMap<Long, Double> ranking_tfidf = new HashMap<>();
        HashMap<Long, Double> ranking_BM25 = new HashMap<>();
        TermInfo termInfo = new TermInfo();
        double tf_tfidf = 0;
        double tf_BM25 = 0;
        for(int i = 0; i < postingArray.length; i++) {
            postingArray[i] = new PostingList();
            termInfo = lexicon.get(query_terms[i]);
            postingArray[i].openList(termInfo);
            System.out.println(query_terms[i] + ": " + postingArray[i].size());
            for(Posting p : postingArray[i]) {
                tf_tfidf = 1 + Math.log(p.getFrequency()) / Math.log(2);
                if(ranking_tfidf.get(p.getDocId()) == null)
                    ranking_tfidf.put(p.getDocId(), tf_tfidf * termInfo.getIdf());
                else
                    ranking_tfidf.put(p.getDocId(), ranking_tfidf.get(p.getDocId()) + tf_tfidf * termInfo.getIdf());

                tf_BM25 = p.getFrequency()/(k1 * ((1-b) + b * ( (double)documentIndex.get(p.getDocId()).getDocLength() / statistics.getAvdl()) + p.getFrequency()));
                if(ranking_BM25.get(p.getDocId()) == null)
                    ranking_BM25.put(p.getDocId(), tf_BM25 * termInfo.getIdf());
                else
                    ranking_BM25.put(p.getDocId(), ranking_tfidf.get(p.getDocId()) + tf_BM25 * termInfo.getIdf());
            }
        }

        System.out.println("\n");
        sortRanking(ranking_tfidf, true);
        sortRanking(ranking_BM25, false);




        // TODO: 04/04/2023 load the postings

        //todo score with daat

        //todo results
    }

    private static void sortRanking(HashMap<Long, Double> ranking, boolean tfidf) {
        System.out.println("------------- " + ((tfidf)?"TFIDF " : "BM25 ") + "RANKING " +  "-------------");
        ArrayList<Double> list = new ArrayList<>();
        LinkedHashMap<Long, Double> sortedMap = new LinkedHashMap<>();

        for (Map.Entry<Long, Double> entry : ranking.entrySet()) {
            list.add(entry.getValue());
        }
        list.sort(Collections.reverseOrder());
        for (double num : list) {
            for (Map.Entry<Long, Double> entry : ranking.entrySet()) {
                if (entry.getValue().equals(num)) {
                    sortedMap.put(entry.getKey(), num);
                }
            }
        }
        List<Long> keys = sortedMap.keySet().stream() .limit(10) .collect(Collectors.toList());
        for(int i = 0; i < keys.size(); i++) {
            System.out.println("#" + (i+1) + " DocID " + keys.get(i) + " -> " + ((tfidf)?"tfidf " : "BM25 ") + "score: " + sortedMap.get(keys.get(i)));
        }
        System.out.println("\n");
    }


}
