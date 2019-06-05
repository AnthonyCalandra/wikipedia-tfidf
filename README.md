# Wikipedia tf-idf

This project creates a tf-idf of Wikipedia articles. The user provides a query of terms they would like to search for, and the application will return the specified articles ordered by descending computed tf-idf. The indexer finds terms in every article and maps them to `List(<byte offset, document frequency, term frequency, article ID>)` where the `byte offset` is the location of the article the term is found in, in the original dataset.

The example dataset I used was retrieved from [here](https://blog.lateral.io/2015/06/the-unknown-perils-of-mining-wikipedia/). `clean.py` was ran on the dataset before feeding it into the indexer.

For example, to search for all articles containing the keywords "big" and "data":
```
$ java -cp target/wikipedia-indexer-1.0.jar com.anthony_calandra.wikipedia_indexer.ArticleRetriever -index wikipedia-index -collection data/wikipedia_utf8_filtered_20pageviews.csv -query "big data AND"
Query: big data AND

tf-idf	Article ID	Article
0.18663820291830818	40008710	Data discovery  Data discovery is a Business intelligence architecture ...
0.1821862684791389	40131150	Data Defined Storage  New advancements in technology, such as growing i...
0.18069245905296735	35458904	Data science  Data science incorporates varying elements and builds on ...
0.1480663058315223	13000371	Data feed  Data feed is a mechanism for users to receive updated data f...
0.141699277026621	34229379	Oracle Big Data Appliance  The Oracle Big Data Appliance consists of ha...
0.13064673814923958	2654483	Enterprise data management  Enterprise Data Management (EDM) is: EDM aro...
0.11763211917592149	168680	Teradata  Teradata Corporation is an American computer company that sells...
0.10724731887453147	39799017	Software-defined data center  Software-defined data center (SDDC) is an...
0.10416278891178483	27051151	Big data  Big data is the term for a collection of data sets so large a...
0.10013070852294043	31882591	RCFile  RCFile (Record Columnar File) is a data placement structure tha...

query completed in 671ms
```
The first column is the article ID (can be queried using Wikipedia's APIs), the second is the article title, and the third is the article contents shortened.

Queries are in reverse Polish notation and can be nested:
```
$ java -cp target/wikipedia-indexer-1.0.jar com.anthony_calandra.wikipedia_indexer.ArticleRetriever -index wikipedia-index -collection data/wikipedia_utf8_filtered_20pageviews.csv -query "big small OR data AND"
Query: big small OR data AND

tf-idf	Article ID	Article
0.2266929587483404	303703	Data mart  A data mart is the access layer of the data warehouse environm...
0.19748924922986472	11658036	Data administration  Data administration or data resource management is...
0.18663820291830818	40008710	Data discovery  Data discovery is a Business intelligence architecture ...
0.1821862684791389	40131150	Data Defined Storage  New advancements in technology, such as growing i...
0.18069245905296735	35458904	Data science  Data science incorporates varying elements and builds on ...
0.16986938808605723	7990	Data warehouse  In computing, a data warehouse or enterprise data warehouse...
0.16152687554931874	988114	Data logger  A data logger (also datalogger or data recorder) is an elect...
0.15628255942761757	1381282	Data loss  Data loss is an error condition in information systems in whi...
0.15370204316370414	466099	Data processing system  A data processing system is a combination of mach...
0.15191481383877772	168753	Data haven  A data haven, like a corporate haven or tax haven, is a refug...

query completed in 1145ms
```
Searches for all articles with contains containing the keywords "big" and "data", or "small" and "data".

## Instructions

Build project:
```
mvn clean package
```

Build the index:
```
hadoop jar target/wikipedia-indexer-1.0.jar \
 com.anthony_calandra.wikipedia_indexer.BuildWikipediaIndex \
 -input data/wikipedia_utf8_filtered_20pageviews.csv \
 -output wikipedia-index -reducers 4
```

Run queries:
```
java -cp target/wikipedia-indexer-1.0.jar \
 com.anthony_calandra.wikipedia_indexer.ArticleRetriever \
 -index wikipedia-index -collection data/wikipedia_utf8_filtered_20pageviews.csv \
 -query "big data AND"
```

## License

MIT

## Author

Anthony Calandra
