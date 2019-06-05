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

package com.anthony_calandra.wikipedia_indexer;

class Article implements Comparable<Article> {
  private long articleIndexOffset;
  private int articleId;
  private double tfidf;

  public Article(long articleIndexOffset, int articleId, double tfidf) {
    this.articleIndexOffset = articleIndexOffset;
    this.articleId = articleId;
    this.tfidf = tfidf;
  }

  public long getArticleIndexOffset() {
    return articleIndexOffset;
  }

  public int getArticleId() {
    return articleId;
  }

  public double getTfidf() {
    return tfidf;
  }

  @Override
  public int hashCode() {
    return articleId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null) return false;
    Article otherArticle = (Article) other;
    if (articleId != otherArticle.articleId) return false;
    return true;
  }

  @Override
  public int compareTo(Article other) {
    return Integer.compare(articleId, other.articleId);
  }
}
