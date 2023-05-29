# SearchEngine

Project for the Multimedia Infrastructure Retrieval course at University of Pisa (A.Y. 2022/2023) realized by Fabiano Pilia and Emanuele Tinghi
Th documents are taken from the following link: [TREC 2020 Deep Learning Track Guidelines | msmarco (microsoft.github.io)](https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020)



### Project structure

The project is composed by these main modules:

- *Inverted-Indexer-Builder*
- *Query-Processor*
- *Query-Evaluator*


#### Inverted-Index-Builder module

Module that performs the indexing (via SPIMI) and merging of the collection

#### Query-Processor module

Module that is responsible of offering the user an interface. Through this interface the user can set some query parameters,
such as the scoring function to use and if the query should be conjunctive or disjunctive. Than she/he can enter the query and see the 
top 20 results relative to it.

The used algorithm to search for the top results is MaxScore


#### Query-Evaluator module
This module performs tests and writes the results in a format suitable for trec_eval


### How to compile the modules

### Indexer module

The *Indexer* module can be compiled using the following optional flags:

As first flag:
- *-c*: if specified, it enables only the *compression* of the document collection
- *-s*: if specified, it enables *stopwords removal and stemming* during documents' processing
- *-sc*: if specified, it enables the *stopwords removal and stemming* of the document collection as well as the *compression* of the resulting data structures
- *-d*: if specified, it enables only the debug mode
- *otherwise*: stopwords removal and stemming are disabled as well as the compression

As second flag (only if the first **is not -d**) the user can enter *-d* to keep the previous flag and also enable the debug mode

The choice made for the first flags will be saved and used in the Query-Processor module


### Query-Processor

Before entering a query the user will be asked which scoring method wants (the choice is between BM25 and TFIDF)
After that the user can choose if the the query to enter needs to be considered conjunctive or disjunctive
As last question the user can select if the query must be executed in debug mode or not

The user can then enter the query. A list of 20 result will be shown, representing the docNo of the highest scoring documents in desconding order

If the user wants to keep the previuusly inserted parameters he/she can enter another query using the right command, otherwise he/she can change the settings
