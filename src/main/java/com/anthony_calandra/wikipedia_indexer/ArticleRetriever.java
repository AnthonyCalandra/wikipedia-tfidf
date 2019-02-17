/**
 * Copyright 2019 Anthony Calandra
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.anthony_calandra.wikipedia_indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfLongInt;
import tl.lin.data.pair.PairOfWritables;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

public class ArticleRetriever {
  private static final Logger LOG = Logger.getLogger(ArticleRetriever.class);
  private MapFile.Reader[] index;
  private FSDataInputStream collection;
  private Stack<Set<Article>> stack;
  private int reducers;
  private int resultLimit;

  private ArticleRetriever() {}

  private void initialize(String indexPath, String collectionPath, int resultLimit, FileSystem fs)
      throws IOException {
    FileStatus[] status = fs.listStatus(new Path(indexPath));
    if (status.length <= 0) {
      throw new IOException("Invalid status length. Possible index missing?");
    }

    index = new MapFile.Reader[status.length - 1]; // Don't include _SUCCESS.
    for (int i = 0; i < status.length; i++) {
      //LOG.info(status[i].getPath().getName());
      if (status[i].getPath().getName().equals("_SUCCESS")) {
        continue;
      }

      // Extract the reducer id from the partition file name. Use that reducer id as the id into the
      // partitioned index. Order matters for lookups because in the index builder step, a term's
      // hashcode is used to partition the article data into one of the partition files.
      int reducerId = Integer.parseInt(status[i].getPath().getName().split("-")[2], 10);
      index[reducerId] = new MapFile.Reader(status[i].getPath(), fs.getConf());
      reducers++;
    }

    collection = fs.open(new Path(collectionPath));
    stack = new Stack<>();
    this.resultLimit = resultLimit;
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");
    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Article> set = stack.pop();
    int results = 0;
    for (Article i : set) {
      if (results == resultLimit) {
        break;
      }

      String line = fetchLine(i.articleIndexOffset);
      System.out.println(line);
      results++;
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Article> s1 = stack.pop();
    Set<Article> s2 = stack.pop();
    Set<Article> sn = new TreeSet<>();
    for (Article n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Article> s1 = stack.pop();
    Set<Article> s2 = stack.pop();
    Set<Article> sn = new TreeSet<>();
    for (Article n : s1) {
      sn.add(n);
    }

    for (Article n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Article> fetchDocumentSet(String term) throws IOException {
    Set<Article> set = new TreeSet<>();
    for (Article article : fetchPostings(term)) {
      set.add(article);
    }

    return set;
  }

  private ArrayList<Article> fetchPostings(String term) throws IOException {
    ArrayList<Article> postList = new ArrayList<>();
    Text key = new Text();
    BytesWritable value = new BytesWritable();
    int partition = (term.hashCode() & Integer.MAX_VALUE) % reducers;

    key.set(term);
    // Term not found in the index.
    if (index[partition].get(key, value) == null) {
      return postList;
    }

    byte[] bytes = value.getBytes();
    ByteArrayInputStream postingByteArrayStream = new ByteArrayInputStream(bytes);
    DataInputStream postingInStream = new DataInputStream(postingByteArrayStream);

    int df = WritableUtils.readVInt(postingInStream);
    long articleIndexOffset = 0;
    for (int numDocs = 0; numDocs < df; numDocs++) {
      long offsetGap = WritableUtils.readVLong(postingInStream);
      int tf = WritableUtils.readVInt(postingInStream);
      int articleId = WritableUtils.readVInt(postingInStream);
      articleIndexOffset += offsetGap;
      postList.add(new Article(articleIndexOffset, articleId, tf));
    }

    return postList;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));
    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;

    @Option(name = "-limit", metaVar = "[num]", usage = "max number of results")
    int resultLimit = 10;
  }

  public static void main(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return;
    }

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return;
    }

    FileSystem fs = FileSystem.get(new Configuration());
    ArticleRetriever inst = new ArticleRetriever();
    inst.initialize(args.index, args.collection, args.resultLimit, fs);
    System.out.println("Query: " + args.query);

    long startTime = System.currentTimeMillis();
    inst.runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");
  }
}
