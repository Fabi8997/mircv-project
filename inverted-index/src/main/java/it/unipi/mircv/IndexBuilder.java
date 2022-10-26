package it.unipi.mircv;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO: 26/10/2022 Write javadocs
public class IndexBuilder {
    HashMap<String, Integer> lexicon;
    HashMap<Integer, ArrayList<Posting>> invertedIndex;
    int currTermID = 1;

    public IndexBuilder() {
        lexicon = new HashMap<>();
        invertedIndex = new HashMap<>();
    }

    public void insertDocument(String processedDocument) {
        int docId;
        String text;

        //Divide the line using \t as delimiter, it'll split the doc_id and the text
        StringTokenizer stringTokenizer = new StringTokenizer(processedDocument, "\t");

        //Retrieve the first token, that is the docno
        if(stringTokenizer.hasMoreTokens()){
            docId = Integer.parseInt(stringTokenizer.nextToken());

            //Retrieve the second token, that is the text and cast it to lower case
            if(stringTokenizer.hasMoreTokens()){
                text = stringTokenizer.nextToken().toLowerCase();
            }else{
                //The text is empty, or it was not possible to retrieve it
                return;
            }
        }else{
            //The line is empty, or it was not possible to retrieve it
            return;
        }

        ArrayList<String> terms = Stream.of(text.split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));

        for(String term : terms){
            if(lexicon.containsKey(term)){

                //update posting list
                ArrayList<Posting> termPostingList = invertedIndex.get(lexicon.get(term));
                boolean found = false;
                for(Posting p : termPostingList){
                    if(p.getDoc_id() == docId){
                        //posting relative to docID already present -> must be updated
                        p.frequency++;
                        found = true;
                    }
                }
                if(!found){
                    //posting relative to docId not present -> must be added
                    termPostingList.add(new Posting(docId, 1));
                }
            }
            else{
                //create a new element in lexicon
                lexicon.put(term, currTermID);

                //create a new posting list in the inverted index
                ArrayList<Posting> postingsList = new ArrayList<>();
                Posting posting = new Posting(docId, 1);
                postingsList.add(posting);
                invertedIndex.put(currTermID, postingsList);

                currTermID++;
            }
        }
    }

    private void clearLexicon(){
        lexicon.clear();
    }

    private void clearInvertedIndex(){
        invertedIndex.clear();
    }

    private void clearTermId(){
        currTermID = 1;
    }

    public void clear(){
        clearLexicon();
        clearInvertedIndex();
        clearTermId();
    }

    public void writeLexiconToFile(){

    }

    public void writeInvertedIndexToFile(){

    }

    public String getLexicon(){
        //Object to build the lexicon into a string
        StringBuilder stringBuilder = new StringBuilder();
        lexicon.forEach((k,v) -> stringBuilder.append(k).append("\t").append(v).append("\n"));
        return  stringBuilder.toString();
    }

    public String[] getInvertedIndex(){
        //Object used to build the list of docids from a posting list
        StringBuilder stringBuilderDocIds = new StringBuilder();

        //Object used to build the list of frequencies from a posting list
        StringBuilder stringBuilderFrequencies = new StringBuilder();

        invertedIndex.forEach((k,v) -> {
                String[] postingList = getPostingList(v);
                stringBuilderDocIds.append(k).append(postingList[0]).append("\n");
                stringBuilderFrequencies.append(k).append(postingList[1]).append("\n");
        });

        return new String[]{ stringBuilderDocIds.toString(), stringBuilderFrequencies.toString()};
    }

    private String[] getPostingList(ArrayList<Posting> postingList){
        StringBuilder stringBuilderDocIds = new StringBuilder();
        StringBuilder stringBuilderFrequencies = new StringBuilder();

        for(Posting posting: postingList){
            stringBuilderDocIds.append(" ").append(posting.getDoc_id());
            stringBuilderFrequencies.append(" ").append(posting.getFrequency());
        }

        return new String[]{ stringBuilderDocIds.toString(), stringBuilderFrequencies.toString()};
    }

    public static void main(String[] args){
        IndexBuilder indexBuilder = new IndexBuilder();
        /*indexBuilder.insertDocument("1\tProva prova ciao");
        indexBuilder.insertDocument("2\tProva posting prova ciao");
        indexBuilder.insertDocument("3\tProva addios prova ciao");*/
        System.out.println(indexBuilder.invertedIndex);
        System.out.println(indexBuilder.getInvertedIndex()[0] + "\n\n" + indexBuilder.getInvertedIndex()[1]);
    }
}
