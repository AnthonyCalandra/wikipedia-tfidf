# Wikipedia Indexer

This project, based on an assignment from school, creates a search index of Wikipedia articles. The user provides a query of keywords they would like to search for, and the application will return the articles that the keywords are found in. The idea behind the indexer is to make search queries fast despite the dataset being large. The indexer is created by using a Hadoop application to read in records (ie. articles) of the input dataset and using the article's contents, providing a mapping of `term => offset, document frequency, term frequency, article ID` where the `offset` is the byte offset into an article from the original dataset.

The example dataset I used was retrieved from [here](https://blog.lateral.io/2015/06/the-unknown-perils-of-mining-wikipedia/). `clean.py` was ran on the dataset before feeding it into the indexer.

For example, to search for all articles containing the keywords "big" and "data":
```
$ java -cp target/wikipedia-indexer-1.0.jar com.anthony_calandra.wikipedia_indexer.ArticleRetriever -index wikipedia-index -collection data/wikipedia_utf8_filtered_20pageviews.csv -query "big data AND"
Query: big data AND
25	Autism  Autism is a disorder of neural development characterized by impaired ...
624	Alaska  Alaska () is a U.S. state situated in the northwest extremity of the...
666	Alkali metal  The alkali metals are a group in the periodic table consisting...
738	Albania  Albania ( , ; Gheg Albanian: ""Shqipni/Shqipnia""), officially know...
791	Asteroid  Asteroids are minor planets, especially those of the inner Solar S...
900	Americium  Americium ( ) is a transuranic radioactive chemical element that ...
922	Anxiety  Anxiety is an unpleasant state of inner turmoil, often accompanied ...
956	Asteraceae  Asteraceae or Compositae (commonly referred to as the aster, dai...
1094	Economy of Armenia  Armenia is the second most densely populated of the for...
1216	Athens  Athens (; , ""Athína"", ; , ""Athēnai"") is the capital and largest...

query completed in 745ms
```
The first column is the article ID (can be queried using Wikipedia's APIs), the second is the article title, and the third is the article contents shortened.

Queries are in reverse Polish notation and can be nested:
```
$ java -cp target/wikipedia-indexer-1.0.jar com.anthony_calandra.wikipedia_indexer.ArticleRetriever -index wikipedia-index -collection data/wikipedia_utf8_filtered_20pageviews.csv -query "big small OR data AND"
Query: big small OR data AND
25	Autism  Autism is a disorder of neural development characterized by impaired ...
308	Aristotle  Aristotle ( , ""Aristotélēs"") (384 BC – 322 BC) was a Greek phil...
586	ASCII  The American Standard Code for Information Interchange (ASCII ) is a ...
621	Amphibian  Amphibians are ectothermic, tetrapod vertebrates of the class Amp...
624	Alaska  Alaska () is a U.S. state situated in the northwest extremity of the...
634	Analysis of variance  Analysis of variance (ANOVA) is a collection of statis...
639	Alkane  In organic chemistry, an alkane, or paraffin (a still-used historica...
655	Abacus  The abacus (""plural"" abaci or abacuses), also called a counting fr...
662	Apollo 11  Apollo 11 was the spaceflight that landed the first humans on the...
663	Apollo 8  Apollo 8, the second manned mission in the United States Apollo sp...

query completed in 497ms
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
