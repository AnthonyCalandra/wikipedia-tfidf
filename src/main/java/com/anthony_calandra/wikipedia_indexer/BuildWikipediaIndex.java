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

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringLong;

import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BuildWikipediaIndex extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildWikipediaIndex.class);

  public static byte[] float2ByteArray(float value) {
    return ByteBuffer.allocate(4).putFloat(value).array();
  }

  public static float byteArray2Float(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getFloat();
  }

  private static final class ArticleMapper extends Mapper<LongWritable, Text, PairOfStringLong, PairOfFloatInt> {
    private static final Object2IntFrequencyDistribution<String> TERM_COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable articleOffset, Text article, Context context)
        throws IOException, InterruptedException {
      String[] articleData = article.toString().split("\\t", 2);
      int articleId = Integer.parseInt(articleData[0]);
      List<String> tokens = Tokenizer.tokenize(articleData[1]);

      // Build a histogram of the terms.
      TERM_COUNTS.clear();
      for (String token : tokens) {
        TERM_COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : TERM_COUNTS) {
        // (term, articleOffset) => (termFrequency, articleId)
        int termFrequency = e.getRightElement();
        context.write(new PairOfStringLong(e.getLeftElement(), articleOffset.get()),
                      new PairOfFloatInt((float) termFrequency / tokens.size(), articleId));
      }
    }
  }

  private static final class ArticleReducer extends Reducer<PairOfStringLong, PairOfFloatInt, Text, BytesWritable> {
    private static final Object2IntFrequencyDistribution<Integer> ARTICLE_COUNTS =
        new Object2IntFrequencyDistributionEntry<>();
    private static final Text TERM = new Text();
    private ByteArrayOutputStream postingByteArrayStream = new ByteArrayOutputStream();
    private DataOutputStream postingOutStream = new DataOutputStream(postingByteArrayStream);
    private String prev;
    private long prevArticleOffset;
    private int df;

    @Override
    public void setup(Context context) {
      prev = null;
      prevArticleOffset = 0;
      df = 0;
      ARTICLE_COUNTS.clear();
    }

    @Override
    public void reduce(PairOfStringLong key, Iterable<PairOfFloatInt> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfFloatInt> iter = values.iterator();
      String term = key.getLeftElement();
      long articleOffset = key.getRightElement();
      if (prev != null && !term.equals(prev)) {
        TERM.set(prev);
        postingOutStream.flush();
        postingByteArrayStream.flush();
        ByteArrayOutputStream mapfileElementByteArrayStream = new ByteArrayOutputStream();
        DataOutputStream mapfileElementOutStream = new DataOutputStream(mapfileElementByteArrayStream);
        WritableUtils.writeVInt(mapfileElementOutStream, df);
        mapfileElementOutStream.write(postingByteArrayStream.toByteArray());
        context.write(TERM, new BytesWritable(mapfileElementByteArrayStream.toByteArray()));
        postingByteArrayStream.reset();
        df = 0;
        prevArticleOffset = 0;
      }

      df++;
      PairOfFloatInt articleData = iter.next();
      if (iter.hasNext()) {
        // Should never get here -- sanity check.
        throw new InterruptedException("Reducer given more than one article.");
      }

      float tf = articleData.getLeftElement();
      int articleId = articleData.getRightElement();
      if (articleOffset - prevArticleOffset < 0) {
        throw new InterruptedException(
          String.format("Document IDs are out of order: %d %d", articleOffset, prevArticleOffset));
      }

      WritableUtils.writeVLong(postingOutStream, articleOffset - prevArticleOffset);
      WritableUtils.writeCompressedByteArray(postingOutStream, float2ByteArray(tf));
      WritableUtils.writeVInt(postingOutStream, articleId);
      ARTICLE_COUNTS.set(articleId, 1);
      prev = term;
      prevArticleOffset = articleOffset;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      if (prev != null) {
        TERM.set(prev);
        postingOutStream.flush();
        postingByteArrayStream.flush();
        ByteArrayOutputStream mapfileElementByteArrayStream = new ByteArrayOutputStream();
        DataOutputStream mapfileElementOutStream = new DataOutputStream(mapfileElementByteArrayStream);
        WritableUtils.writeVInt(mapfileElementOutStream, df);
        mapfileElementOutStream.write(postingByteArrayStream.toByteArray());
        context.write(TERM, new BytesWritable(mapfileElementByteArrayStream.toByteArray()));
        postingByteArrayStream.reset();
      }

      // Store total number of articles.
      // Setting the term to a tilde is a really hacky way of doing this...
      TERM.set("~");
      postingOutStream.flush();
      postingByteArrayStream.flush();
      ByteArrayOutputStream mapfileElementByteArrayStream = new ByteArrayOutputStream();
      DataOutputStream mapfileElementOutStream = new DataOutputStream(mapfileElementByteArrayStream);
      WritableUtils.writeVInt(mapfileElementOutStream, ARTICLE_COUNTS.getNumberOfEvents());
      context.write(TERM, new BytesWritable(mapfileElementByteArrayStream.toByteArray()));

      postingOutStream.close();
      postingByteArrayStream.close();
    }
  }

  private static final class ArticlePartitioner extends Partitioner<PairOfStringLong, PairOfFloatInt> {
    @Override
    public int getPartition(PairOfStringLong key, PairOfFloatInt value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildWikipediaIndex() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
  }

  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildWikipediaIndex.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildWikipediaIndex.class.getSimpleName());
    job.setJarByClass(BuildWikipediaIndex.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringLong.class);
    job.setMapOutputValueClass(PairOfFloatInt.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(ArticleMapper.class);
    job.setReducerClass(ArticleReducer.class);
    job.setPartitionerClass(ArticlePartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildWikipediaIndex(), args);
  }
}
