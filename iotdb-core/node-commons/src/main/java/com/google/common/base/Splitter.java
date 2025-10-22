/*
 * Copyright (C) 2009 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.base;

import javax.annotation.CheckForNull;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extracts non-overlapping substrings from an input string, typically by recognizing appearances of
 * a <i>separator</i> sequence. This separator can be specified as a single {@linkplain #on(char)
 * character}, fixed {@linkplain #on(String) string}, {@linkplain #onPattern regular expression} or
 * {@link #on(CharMatcher) CharMatcher} instance. Or, instead of using a separator at all, a
 * splitter can extract adjacent substrings of a given {@linkplain #fixedLength fixed length}.
 *
 * <p>For example, this expression:
 *
 * <pre>{@code
 * Splitter.on(',').split("foo,bar,qux")
 * }</pre>
 *
 * ... produces an {@code Iterable} containing {@code "foo"}, {@code "bar"} and {@code "qux"}, in
 * that order.
 *
 * <p>By default, {@code Splitter}'s behavior is simplistic and unassuming. The following
 * expression:
 *
 * <pre>{@code
 * Splitter.on(',').split(" foo,,,  bar ,")
 * }</pre>
 *
 * ... yields the substrings {@code [" foo", "", "", " bar ", ""]}. If this is not the desired
 * behavior, use configuration methods to obtain a <i>new</i> splitter instance with modified
 * behavior:
 *
 * <pre>{@code
 * private static final Splitter MY_SPLITTER = Splitter.on(',')
 *     .trimResults()
 *     .omitEmptyStrings();
 * }</pre>
 *
 * <p>Now {@code MY_SPLITTER.split("foo,,, bar ,")} returns just {@code ["foo", "bar"]}. Note that
 * the order in which these configuration methods are called is never significant.
 *
 * <p><b>Warning:</b> Splitter instances are immutable. Invoking a configuration method has no
 * effect on the receiving instance; you must store and use the new splitter instance it returns
 * instead.
 *
 * <pre>{@code
 * // Do NOT do this
 * Splitter splitter = Splitter.on('/');
 * splitter.trimResults(); // does nothing!
 * return splitter.split("wrong / wrong / wrong");
 * }</pre>
 *
 * <p>For separator-based splitters that do not use {@code omitEmptyStrings}, an input string
 * containing {@code n} occurrences of the separator naturally yields an iterable of size {@code n +
 * 1}. So if the separator does not occur anywhere in the input, a single substring is returned
 * containing the entire input. Consequently, all splitters split the empty string to {@code [""]}
 * (note: even fixed-length splitters).
 *
 * <p>Splitter instances are thread-safe immutable, and are therefore safe to store as {@code static
 * final} constants.
 *
 * <p>The {@link Joiner} class provides the inverse operation to splitting, but note that a
 * round-trip between the two should be assumed to be lossy.
 *
 * <p>See the Guava User Guide article on <a
 * href="https://github.com/google/guava/wiki/StringsExplained#splitter">{@code Splitter}</a>.
 *
 * @author Julien Silland
 * @author Jesse Wilson
 * @author Kevin Bourrillion
 * @author Louis Wasserman
 * @since 1.0
 */
public final class Splitter {
  private final CharMatcher trimmer;
  private final boolean omitEmptyStrings;
  private final Strategy strategy;
  private final int limit;

  private Splitter(Strategy strategy) {
    this(strategy, false, CharMatcher.none(), Integer.MAX_VALUE);
  }

  private Splitter(Strategy strategy, boolean omitEmptyStrings, CharMatcher trimmer, int limit) {
    this.strategy = strategy;
    this.omitEmptyStrings = omitEmptyStrings;
    this.trimmer = trimmer;
    this.limit = limit;
  }

  /**
   * Returns a splitter that uses the given single-character separator. For example, {@code
   * Splitter.on(',').split("foo,,bar")} returns an iterable containing {@code ["foo", "", "bar"]}.
   *
   * @param separator the character to recognize as a separator
   * @return a splitter, with default settings, that recognizes that separator
   */
  public static Splitter on(char separator) {
    return on(CharMatcher.is(separator));
  }

  /**
   * Returns a splitter that considers any single character matched by the given {@code CharMatcher}
   * to be a separator. For example, {@code
   * Splitter.on(CharMatcher.anyOf(";,")).split("foo,;bar,quux")} returns an iterable containing
   * {@code ["foo", "", "bar", "quux"]}.
   *
   * @param separatorMatcher a {@link CharMatcher} that determines whether a character is a
   *     separator
   * @return a splitter, with default settings, that uses this matcher
   */
  public static Splitter on(final CharMatcher separatorMatcher) {
    checkNotNull(separatorMatcher);

    return new Splitter(
        new Strategy() {
          @Override
          public SplittingIterator iterator(Splitter splitter, final CharSequence toSplit) {
            return new SplittingIterator(splitter, toSplit) {
              @Override
              int separatorStart(int start) {
                return separatorMatcher.indexIn(toSplit, start);
              }

              @Override
              int separatorEnd(int separatorPosition) {
                return separatorPosition + 1;
              }
            };
          }
        });
  }

  /**
   * Returns a splitter that uses the given fixed string as a separator. For example, {@code
   * Splitter.on(", ").split("foo, bar,baz")} returns an iterable containing {@code ["foo",
   * "bar,baz"]}.
   *
   * @param separator the literal, nonempty string to recognize as a separator
   * @return a splitter, with default settings, that recognizes that separator
   */
  public static Splitter on(final String separator) {
    checkArgument(separator.length() != 0, "The separator may not be the empty string.");
    if (separator.length() == 1) {
      return Splitter.on(separator.charAt(0));
    }
    return new Splitter(
        new Strategy() {
          @Override
          public SplittingIterator iterator(Splitter splitter, CharSequence toSplit) {
            return new SplittingIterator(splitter, toSplit) {
              @Override
              public int separatorStart(int start) {
                int separatorLength = separator.length();

                positions:
                for (int p = start, last = toSplit.length() - separatorLength; p <= last; p++) {
                  for (int i = 0; i < separatorLength; i++) {
                    if (toSplit.charAt(i + p) != separator.charAt(i)) {
                      continue positions;
                    }
                  }
                  return p;
                }
                return -1;
              }

              @Override
              public int separatorEnd(int separatorPosition) {
                return separatorPosition + separator.length();
              }
            };
          }
        });
  }

  /**
   * Returns a splitter that considers any subsequence matching {@code pattern} to be a separator.
   * For example, {@code Splitter.on(Pattern.compile("\r?\n")).split(entireFile)} splits a string
   * into lines whether it uses DOS-style or UNIX-style line terminators.
   *
   * @param separatorPattern the pattern that determines whether a subsequence is a separator. This
   *     pattern may not match the empty string.
   * @return a splitter, with default settings, that uses this pattern
   * @throws IllegalArgumentException if {@code separatorPattern} matches the empty string
   */
  public static Splitter on(Pattern separatorPattern) {
    return on(new JdkPattern(separatorPattern));
  }

  private static Splitter on(final CommonPattern separatorPattern) {
    checkArgument(
        !separatorPattern.matcher("").matches(),
        "The pattern may not match the empty string: %s",
        separatorPattern);

    return new Splitter(
        new Strategy() {
          @Override
          public SplittingIterator iterator(final Splitter splitter, CharSequence toSplit) {
            final CommonMatcher matcher = separatorPattern.matcher(toSplit);
            return new SplittingIterator(splitter, toSplit) {
              @Override
              public int separatorStart(int start) {
                return matcher.find(start) ? matcher.start() : -1;
              }

              @Override
              public int separatorEnd(int separatorPosition) {
                return matcher.end();
              }
            };
          }
        });
  }

  /**
   * Returns a splitter that behaves equivalently to {@code this} splitter but stops splitting after
   * it reaches the limit. The limit defines the maximum number of items returned by the iterator,
   * or the maximum size of the list returned by {@link #splitToList}.
   *
   * <p>For example, {@code Splitter.on(',').limit(3).split("a,b,c,d")} returns an iterable
   * containing {@code ["a", "b", "c,d"]}. When omitting empty strings, the omitted strings do not
   * count. Hence, {@code Splitter.on(',').limit(3).omitEmptyStrings().split("a,,,b,,,c,d")} returns
   * an iterable containing {@code ["a", "b", "c,d"}. When trim is requested, all entries are
   * trimmed, including the last. Hence {@code Splitter.on(',').limit(3).trimResults().split(" a , b
   * , c , d ")} results in {@code ["a", "b", "c , d"]}.
   *
   * @param maxItems the maximum number of items returned
   * @return a splitter with the desired configuration
   * @since 9.0
   */
  public Splitter limit(int maxItems) {
    checkArgument(maxItems > 0, "must be greater than zero: %s", maxItems);
    return new Splitter(strategy, omitEmptyStrings, trimmer, maxItems);
  }

  /**
   * Returns a splitter that behaves equivalently to {@code this} splitter, but automatically
   * removes leading and trailing {@linkplain CharMatcher#whitespace whitespace} from each returned
   * substring; equivalent to {@code trimResults(CharMatcher.whitespace())}. For example, {@code
   * Splitter.on(',').trimResults().split(" a, b ,c ")} returns an iterable containing {@code ["a",
   * "b", "c"]}.
   *
   * @return a splitter with the desired configuration
   */
  public Splitter trimResults() {
    return trimResults(CharMatcher.whitespace());
  }

  /**
   * Returns a splitter that behaves equivalently to {@code this} splitter, but removes all leading
   * or trailing characters matching the given {@code CharMatcher} from each returned substring. For
   * example, {@code Splitter.on(',').trimResults(CharMatcher.is('_')).split("_a ,_b_ ,c__")}
   * returns an iterable containing {@code ["a ", "b_ ", "c"]}.
   *
   * @param trimmer a {@link CharMatcher} that determines whether a character should be removed from
   *     the beginning/end of a subsequence
   * @return a splitter with the desired configuration
   */
  // TODO(kevinb): throw if a trimmer was already specified!
  public Splitter trimResults(CharMatcher trimmer) {
    checkNotNull(trimmer);
    return new Splitter(strategy, omitEmptyStrings, trimmer, limit);
  }

  /**
   * Splits {@code sequence} into string components and makes them available through an {@link
   * Iterator}, which may be lazily evaluated. If you want an eagerly computed {@link List}, use
   * {@link #splitToList(CharSequence)}. Java 8 users may prefer {@link #splitToStream} instead.
   *
   * @param sequence the sequence of characters to split
   * @return an iteration over the segments split from the parameter
   */
  public Iterable<String> split(final CharSequence sequence) {
    checkNotNull(sequence);

    return new Iterable<String>() {
      @Override
      public Iterator<String> iterator() {
        return splittingIterator(sequence);
      }

      @Override
      public String toString() {
        return Joiner.on(", ")
            .appendTo(new StringBuilder().append('['), this)
            .append(']')
            .toString();
      }
    };
  }

  private Iterator<String> splittingIterator(CharSequence sequence) {
    return strategy.iterator(this, sequence);
  }

  private interface Strategy {
    Iterator<String> iterator(Splitter splitter, CharSequence toSplit);
  }

  private abstract static class SplittingIterator extends AbstractIterator<String> {
    final CharSequence toSplit;
    final CharMatcher trimmer;
    final boolean omitEmptyStrings;

    /**
     * Returns the first index in {@code toSplit} at or after {@code start} that contains the
     * separator.
     */
    abstract int separatorStart(int start);

    /**
     * Returns the first index in {@code toSplit} after {@code separatorPosition} that does not
     * contain a separator. This method is only invoked after a call to {@code separatorStart}.
     */
    abstract int separatorEnd(int separatorPosition);

    int offset = 0;
    int limit;

    protected SplittingIterator(Splitter splitter, CharSequence toSplit) {
      this.trimmer = splitter.trimmer;
      this.omitEmptyStrings = splitter.omitEmptyStrings;
      this.limit = splitter.limit;
      this.toSplit = toSplit;
    }

    @CheckForNull
    @Override
    protected String computeNext() {
      /*
       * The returned string will be from the end of the last match to the beginning of the next
       * one. nextStart is the start position of the returned substring, while offset is the place
       * to start looking for a separator.
       */
      int nextStart = offset;
      while (offset != -1) {
        int start = nextStart;
        int end;

        int separatorPosition = separatorStart(offset);
        if (separatorPosition == -1) {
          end = toSplit.length();
          offset = -1;
        } else {
          end = separatorPosition;
          offset = separatorEnd(separatorPosition);
        }
        if (offset == nextStart) {
          /*
           * This occurs when some pattern has an empty match, even if it doesn't match the empty
           * string -- for example, if it requires lookahead or the like. The offset must be
           * increased to look for separators beyond this point, without changing the start position
           * of the next returned substring -- so nextStart stays the same.
           */
          offset++;
          if (offset > toSplit.length()) {
            offset = -1;
          }
          continue;
        }

        while (start < end && trimmer.matches(toSplit.charAt(start))) {
          start++;
        }
        while (end > start && trimmer.matches(toSplit.charAt(end - 1))) {
          end--;
        }

        if (omitEmptyStrings && start == end) {
          // Don't include the (unused) separator in next split string.
          nextStart = offset;
          continue;
        }

        if (limit == 1) {
          // The limit has been reached, return the rest of the string as the
          // final item. This is tested after empty string removal so that
          // empty strings do not count towards the limit.
          end = toSplit.length();
          offset = -1;
          // Since we may have changed the end, we need to trim it again.
          while (end > start && trimmer.matches(toSplit.charAt(end - 1))) {
            end--;
          }
        } else {
          limit--;
        }

        return toSplit.subSequence(start, end).toString();
      }
      return endOfData();
    }
  }
}
