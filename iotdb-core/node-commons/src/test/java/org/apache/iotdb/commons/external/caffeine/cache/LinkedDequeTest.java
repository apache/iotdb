/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.external.caffeine.cache;

import org.apache.iotdb.commons.external.caffeine.cache.AccessOrderDeque.AccessOrder;
import org.apache.iotdb.commons.external.caffeine.cache.LinkedDeque.PeekingIterator;
import org.apache.iotdb.commons.external.caffeine.cache.WriteOrderDeque.WriteOrder;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.external.caffeine.cache.testing.TruthAssert.assertThat;
import static org.junit.Assert.assertThrows;

/**
 * A unit-test for the @{@link AbstractLinkedDeque} implementations.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings({"ClassEscapesDefinedScope", "PMD.LooseCoupling"})
public final class LinkedDequeTest {
  static final int SIZE = 100;

  @Test
  public void clear_whenEmpty() {
    empty().forEach(d -> clear_whenEmptyImpl(d));
  }

  private void clear_whenEmptyImpl(Deque<?> deque) {
    deque.clear();
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void clear_whenPopulated() {
    full().forEach(d -> clear_whenPopulatedImpl(d));
  }

  private void clear_whenPopulatedImpl(Deque<?> deque) {
    deque.clear();
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void isEmpty_whenEmpty() {
    empty().forEach(d -> isEmpty_whenEmptyImpl(d));
  }

  private void isEmpty_whenEmptyImpl(Deque<?> deque) {
    assertThat(deque.isEmpty()).isTrue();
  }

  @Test
  public void isEmpty_whenPopulated() {
    full().forEach(d -> isEmpty_whenPopulatedImpl(d));
  }

  private void isEmpty_whenPopulatedImpl(Deque<?> deque) {
    assertThat(deque.isEmpty()).isFalse();
  }

  @Test
  public void size_whenEmpty() {
    empty().forEach(d -> size_whenEmptyImpl(d));
  }

  private void size_whenEmptyImpl(Deque<?> deque) {
    assertThat(deque.size()).isEqualTo(0);
  }

  @Test
  public void size_whenPopulated() {
    full().forEach(d -> size_whenPopulatedImpl(d));
  }

  private void size_whenPopulatedImpl(Deque<?> deque) {
    assertThat(deque.size()).isEqualTo(SIZE);
  }

  @Test
  public void contains_withNull() {
    empty().forEach(d -> contains_withNullImpl(d));
  }

  private void contains_withNullImpl(Deque<?> deque) {
    assertThat(deque.contains(null)).isFalse();
  }

  @Test
  public void contains_whenFound() {
    full().forEach(d -> contains_whenFoundImpl(d));
  }

  private void contains_whenFoundImpl(LinkedDeque<LinkedValue> deque) {
    int index = ThreadLocalRandom.current().nextInt(deque.size());
    assertThat(deque.contains(getAt(deque, index))).isTrue();
  }

  @Test
  public void contains_whenNotFound() {
    full().forEach(d -> contains_whenNotFoundImpl(d));
  }

  private void contains_whenNotFoundImpl(LinkedDeque<LinkedValue> deque) {
    var unlinked = new LinkedValue(1);
    assertThat(deque.contains(unlinked)).isFalse();
  }

  /* --------------- Move --------------- */

  @Test
  public void moveToFront_first() {
    full().forEach(d -> moveToFront_firstImpl(d));
  }

  private void moveToFront_firstImpl(LinkedDeque<LinkedValue> deque) {
    checkMoveToFront(deque, deque.getFirst());
  }

  @Test
  public void moveToFront_middle() {
    full().forEach(d -> moveToFront_middleImpl(d));
  }

  private void moveToFront_middleImpl(LinkedDeque<LinkedValue> deque) {
    checkMoveToFront(deque, getAt(deque, SIZE / 2));
  }

  @Test
  public void moveToFront_last() {
    full().forEach(d -> moveToFront_lastImpl(d));
  }

  private void moveToFront_lastImpl(LinkedDeque<LinkedValue> deque) {
    checkMoveToFront(deque, deque.getLast());
  }

  private static void checkMoveToFront(LinkedDeque<LinkedValue> deque, LinkedValue element) {
    assertThat(element).isNotNull();
    deque.moveToFront(element);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.peekFirst()).isEqualTo(element);
  }

  @Test
  public void moveToBack_first() {
    full().forEach(d -> moveToBack_firstImpl(d));
  }

  private void moveToBack_firstImpl(LinkedDeque<LinkedValue> deque) {
    checkMoveToBack(deque, deque.getFirst());
  }

  @Test
  public void moveToBack_middle() {
    full().forEach(d -> moveToBack_middleImpl(d));
  }

  private void moveToBack_middleImpl(LinkedDeque<LinkedValue> deque) {
    checkMoveToBack(deque, getAt(deque, SIZE / 2));
  }

  @Test
  public void moveToBack_last() {
    full().forEach(d -> moveToBack_lastImpl(d));
  }

  private void moveToBack_lastImpl(LinkedDeque<LinkedValue> deque) {
    checkMoveToBack(deque, deque.getLast());
  }

  private static void checkMoveToBack(LinkedDeque<LinkedValue> deque, LinkedValue element) {
    assertThat(element).isNotNull();
    deque.moveToBack(element);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.getLast()).isEqualTo(element);
  }

  /* --------------- First / Last --------------- */

  @Test
  public void isFirst_whenEmpty() {
    empty().forEach(d -> isFirst_whenEmptyImpl(d));
  }

  private void isFirst_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.isFirst(new LinkedValue(0))).isFalse();
    assertThat(deque.isFirst(null)).isFalse();
  }

  @Test
  public void isFirst_whenPopulated() {
    full().forEach(d -> isFirst_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  private void isFirst_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var first = requireNonNull(deque.first);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.isFirst(first)).isTrue();
    assertThat(deque.contains(first)).isTrue();
    assertThat(deque.first).isSameInstanceAs(first);
  }

  @Test
  public void isLast_whenEmpty() {
    empty().forEach(d -> isLast_whenEmptyImpl(d));
  }

  private void isLast_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.isLast(new LinkedValue(0))).isFalse();
    assertThat(deque.isLast(null)).isFalse();
  }

  @Test
  public void isLast_whenPopulated() {
    full().forEach(d -> isLast_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  private void isLast_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var last = requireNonNull(deque.last);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.isLast(last)).isTrue();
    assertThat(deque.contains(last)).isTrue();
    assertThat(deque.last).isSameInstanceAs(last);
  }

  /* --------------- Peek --------------- */

  @Test
  public void peek_whenEmpty() {
    empty().forEach(d -> peek_whenEmptyImpl(d));
  }

  @SuppressWarnings("DequePeekFirst")
  private void peek_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.peek()).isNull();
  }

  @Test
  public void peek_whenPopulated() {
    full().forEach(d -> peek_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  @SuppressWarnings("DequePeekFirst")
  private void peek_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var first = requireNonNull(deque.first);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.contains(first)).isTrue();
    assertThat(deque.first).isSameInstanceAs(first);
    assertThat(deque.peek()).isSameInstanceAs(first);
  }

  @Test
  public void peekFirst_whenEmpty() {
    empty().forEach(d -> peekFirst_whenEmptyImpl(d));
  }

  private void peekFirst_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.peekFirst()).isNull();
  }

  @Test
  public void peekFirst_whenPopulated() {
    full().forEach(d -> peekFirst_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  private void peekFirst_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var first = requireNonNull(deque.first);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.contains(first)).isTrue();
    assertThat(deque.first).isSameInstanceAs(first);
    assertThat(deque.peekFirst()).isSameInstanceAs(first);
  }

  @Test
  public void peekLast_whenEmpty() {
    empty().forEach(d -> peekLast_whenEmptyImpl(d));
  }

  private void peekLast_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.peekLast()).isNull();
  }

  @Test
  public void peekLast_whenPopulated() {
    full().forEach(d -> peekLast_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  private void peekLast_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var last = requireNonNull(deque.last);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.contains(last)).isTrue();
    assertThat(deque.last).isSameInstanceAs(last);
    assertThat(deque.peekLast()).isSameInstanceAs(last);
  }

  /* --------------- Get --------------- */

  @Test
  public void getFirst_whenEmpty() {
    empty().forEach(d -> getFirst_whenEmptyImpl(d));
  }

  private void getFirst_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThrows(NoSuchElementException.class, deque::getFirst);
  }

  @Test
  public void getFirst_whenPopulated() {
    full().forEach(d -> getFirst_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  private void getFirst_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var first = requireNonNull(deque.first);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.contains(first)).isTrue();
    assertThat(deque.first).isSameInstanceAs(first);
    assertThat(deque.getFirst()).isSameInstanceAs(first);
  }

  @Test
  public void getLast_whenEmpty() {
    empty().forEach(d -> getLast_whenEmptyImpl(d));
  }

  private void getLast_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThrows(NoSuchElementException.class, deque::getLast);
  }

  @Test
  public void getLast_whenPopulated() {
    full().forEach(d -> getLast_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  private void getLast_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var last = requireNonNull(deque.last);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.contains(last)).isTrue();
    assertThat(deque.last).isSameInstanceAs(last);
    assertThat(deque.getLast()).isSameInstanceAs(last);
  }

  /* --------------- Element --------------- */

  @Test
  public void element_whenEmpty() {
    empty().forEach(d -> element_whenEmptyImpl(d));
  }

  private void element_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThrows(NoSuchElementException.class, deque::element);
  }

  @Test
  public void element_whenPopulated() {
    full().forEach(d -> element_whenPopulatedImpl((AbstractLinkedDeque<LinkedValue>) d));
  }

  @SuppressWarnings("DequeGetFirst")
  private void element_whenPopulatedImpl(AbstractLinkedDeque<LinkedValue> deque) {
    var first = requireNonNull(deque.first);
    assertThat(deque).hasSize(SIZE);
    assertThat(deque.contains(first)).isTrue();
    assertThat(deque.first).isSameInstanceAs(first);
    assertThat(deque.element()).isSameInstanceAs(first);
  }

  /* --------------- Offer --------------- */

  @Test
  public void offer_whenEmpty() {
    empty().forEach(d -> offer_whenEmptyImpl(d));
  }

  @SuppressWarnings("DequeOfferLast")
  private void offer_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(1);
    assertThat(deque.offer(value)).isTrue();
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(1);
  }

  @Test
  public void offer_whenPopulated() {
    full().forEach(d -> offer_whenPopulatedImpl(d));
  }

  @SuppressWarnings("DequeOfferLast")
  private void offer_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(SIZE);
    assertThat(deque.offer(value)).isTrue();
    assertThat(deque.peekFirst()).isNotSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(SIZE + 1);
  }

  @Test
  public void offer_whenLinked() {
    full().forEach(d -> offer_whenLinkedImpl(d));
  }

  @SuppressWarnings("DequeOfferLast")
  private void offer_whenLinkedImpl(LinkedDeque<LinkedValue> deque) {
    var value = requireNonNull(deque.peekFirst());
    assertThat(deque.offer(value)).isFalse();
    assertThat(deque).hasSize(SIZE);
  }

  @Test
  public void offerFirst_whenEmpty() {
    empty().forEach(d -> offerFirst_whenEmptyImpl(d));
  }

  private void offerFirst_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(1);
    assertThat(deque.offerFirst(value)).isTrue();
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(1);
  }

  @Test
  public void offerFirst_whenPopulated() {
    full().forEach(d -> offerFirst_whenPopulatedImpl(d));
  }

  private void offerFirst_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(SIZE);
    assertThat(deque.offerFirst(value)).isTrue();
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isNotSameInstanceAs(value);
    assertThat(deque).hasSize(SIZE + 1);
  }

  @Test
  public void offerFirst_whenLinked() {
    full().forEach(d -> offerFirst_whenLinkedImpl(d));
  }

  private void offerFirst_whenLinkedImpl(LinkedDeque<LinkedValue> deque) {
    var value = requireNonNull(deque.peekFirst());
    assertThat(deque.offerFirst(value)).isFalse();
    assertThat(deque).hasSize(SIZE);
  }

  @Test
  public void offerLast_whenEmpty() {
    empty().forEach(d -> offerLast_whenEmptyImpl(d));
  }

  private void offerLast_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(1);
    assertThat(deque.offerLast(value)).isTrue();
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(1);
  }

  @Test
  public void offerLast_whenPopulated() {
    full().forEach(d -> offerLast_whenPopulatedImpl(d));
  }

  private void offerLast_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(SIZE);
    assertThat(deque.offerLast(value)).isTrue();
    assertThat(deque.peekFirst()).isNotSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(SIZE + 1);
  }

  @Test
  public void offerLast_whenLinked() {
    full().forEach(d -> offerLast_whenLinkedImpl(d));
  }

  private void offerLast_whenLinkedImpl(LinkedDeque<LinkedValue> deque) {
    var value = requireNonNull(deque.peekFirst());
    assertThat(deque.offerLast(value)).isFalse();
    assertThat(deque).hasSize(SIZE);
  }

  /* --------------- Add --------------- */

  @Test
  public void add_whenEmpty() {
    empty().forEach(d -> add_whenEmptyImpl(d));
  }

  private void add_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(1);
    assertThat(deque.add(value)).isTrue();
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(1);
  }

  @Test
  public void add_whenPopulated() {
    full().forEach(d -> add_whenPopulatedImpl(d));
  }

  private void add_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(SIZE);
    assertThat(deque.add(value)).isTrue();
    assertThat(deque.peekFirst()).isNotSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(SIZE + 1);
  }

  @Test
  public void add_whenLinked() {
    full().forEach(d -> add_whenLinkedImpl(d));
  }

  private void add_whenLinkedImpl(LinkedDeque<LinkedValue> deque) {
    var value = requireNonNull(deque.peekFirst());
    assertThat(deque.add(value)).isFalse();
  }

  @Test
  public void addFirst_whenEmpty() {
    empty().forEach(d -> addFirst_whenEmptyImpl(d));
  }

  private void addFirst_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(1);
    deque.addFirst(value);
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(1);
  }

  @Test
  public void addFirst_whenPopulated() {
    full().forEach(d -> addFirst_whenPopulatedImpl(d));
  }

  private void addFirst_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(SIZE);
    deque.addFirst(value);
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isNotSameInstanceAs(value);
    assertThat(deque).hasSize(SIZE + 1);
  }

  @Test
  public void addFirst_whenLinked() {
    full().forEach(d -> addFirst_whenLinkedImpl(d));
  }

  private void addFirst_whenLinkedImpl(LinkedDeque<LinkedValue> deque) {
    var value = requireNonNull(deque.peekFirst());
    assertThrows(IllegalArgumentException.class, () -> deque.addFirst(value));
  }

  @Test
  public void addLast_whenEmpty() {
    empty().forEach(d -> addLast_whenEmptyImpl(d));
  }

  private void addLast_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(1);
    deque.addLast(value);
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(1);
  }

  @Test
  public void addLast_whenPopulated() {
    full().forEach(d -> addLast_whenPopulatedImpl(d));
  }

  private void addLast_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(SIZE);
    deque.addLast(value);
    assertThat(deque.peekFirst()).isNotSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(SIZE + 1);
  }

  @Test
  public void addLast_whenLinked() {
    full().forEach(d -> addLast_whenLinkedImpl(d));
  }

  private void addLast_whenLinkedImpl(LinkedDeque<LinkedValue> deque) {
    var value = requireNonNull(deque.peekFirst());
    assertThrows(IllegalArgumentException.class, () -> deque.addLast(value));
  }

  @Test
  public void addAll_withEmpty() {
    empty().forEach(d -> addAll_withEmptyImpl(d));
  }

  private void addAll_withEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.addAll(List.of())).isFalse();
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void addAll_withPopulated() {
    empty().forEach(d -> addAll_withPopulatedImpl(d));
  }

  private void addAll_withPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var expected = new ArrayList<LinkedValue>();
    populate(expected);
    assertThat(deque.addAll(expected)).isTrue();
    assertThat(deque).containsExactlyElementsIn(expected).inOrder();
  }

  @Test
  public void addAll_withSelf() {
    full().forEach(d -> addAll_withSelfImpl(d));
  }

  @SuppressWarnings({"CollectionAddedToSelf", "ModifyingCollectionWithItself"})
  private void addAll_withSelfImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.addAll(deque)).isFalse();
  }

  /* --------------- Poll --------------- */

  @Test
  public void poll_whenEmpty() {
    empty().forEach(d -> poll_whenEmptyImpl(d));
  }

  @SuppressWarnings("DequePollFirst")
  private void poll_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.poll()).isNull();
  }

  @Test
  public void poll_whenPopulated() {
    full().forEach(d -> poll_whenPopulatedImpl(d));
  }

  @SuppressWarnings("DequePollFirst")
  private void poll_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.poll()).isSameInstanceAs(first);
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void poll_toEmpty() {
    full().forEach(d -> poll_toEmptyImpl(d));
  }

  @SuppressWarnings("DequePollFirst")
  private void poll_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    LinkedValue value;
    while ((value = deque.poll()) != null) {
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void pollFirst_whenEmpty() {
    empty().forEach(d -> pollFirst_whenEmptyImpl(d));
  }

  private void pollFirst_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.pollFirst()).isNull();
  }

  @Test
  public void pollFirst_whenPopulated() {
    full().forEach(d -> pollFirst_whenPopulatedImpl(d));
  }

  private void pollFirst_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.pollFirst()).isSameInstanceAs(first);
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void pollFirst_toEmpty() {
    full().forEach(d -> pollFirst_toEmptyImpl(d));
  }

  private void pollFirst_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    LinkedValue value;
    while ((value = deque.pollFirst()) != null) {
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void pollLast_whenEmpty() {
    empty().forEach(d -> pollLast_whenEmptyImpl(d));
  }

  private void pollLast_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.pollLast()).isNull();
  }

  @Test
  public void pollLast_whenPopulated() {
    full().forEach(d -> pollLast_whenPopulatedImpl(d));
  }

  private void pollLast_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var last = deque.peekLast();
    assertThat(deque.pollLast()).isSameInstanceAs(last);
    assertThat(deque.contains(last)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void pollLast_toEmpty() {
    full().forEach(d -> pollLast_toEmptyImpl(d));
  }

  private void pollLast_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    LinkedValue value;
    while ((value = deque.pollLast()) != null) {
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  /* --------------- Remove --------------- */

  @Test
  public void remove_whenEmpty() {
    empty().forEach(d -> remove_whenEmptyImpl(d));
  }

  private void remove_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThrows(NoSuchElementException.class, deque::remove);
  }

  @Test
  public void remove_whenPopulated() {
    full().forEach(d -> remove_whenPopulatedImpl(d));
  }

  @SuppressWarnings("DequeRemoveFirst")
  private void remove_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.remove()).isSameInstanceAs(first);
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void remove_toEmpty() {
    full().forEach(d -> remove_toEmptyImpl(d));
  }

  @SuppressWarnings("DequeRemoveFirst")
  private void remove_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    while (!deque.isEmpty()) {
      var value = deque.remove();
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeElement_notFound() {
    empty().forEach(d -> removeElement_notFoundImpl(d));
  }

  @SuppressWarnings("DequeRemoveFirstOccurrence")
  private void removeElement_notFoundImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.remove(new LinkedValue(0))).isFalse();
  }

  @Test
  public void removeElement_whenFound() {
    full().forEach(d -> removeElement_whenFoundImpl(d));
  }

  @SuppressWarnings("DequeRemoveFirstOccurrence")
  private void removeElement_whenFoundImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.remove(first)).isTrue();
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void removeElement_toEmpty() {
    full().forEach(d -> removeElement_toEmptyImpl(d));
  }

  @SuppressWarnings("DequeRemoveFirstOccurrence")
  private void removeElement_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    while (!deque.isEmpty()) {
      var value = deque.peekFirst();
      assertThat(deque.remove(value)).isTrue();
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeFirst_whenEmpty() {
    empty().forEach(d -> removeFirst_whenEmptyImpl(d));
  }

  private void removeFirst_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThrows(NoSuchElementException.class, deque::removeFirst);
  }

  @Test
  public void removeFirst_whenPopulated() {
    full().forEach(d -> removeFirst_whenPopulatedImpl(d));
  }

  private void removeFirst_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.removeFirst()).isSameInstanceAs(first);
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void removeFirst_toEmpty() {
    full().forEach(d -> removeFirst_toEmptyImpl(d));
  }

  private void removeFirst_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    while (!deque.isEmpty()) {
      var value = deque.removeFirst();
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeLast_whenEmpty() {
    empty().forEach(d -> removeLast_whenEmptyImpl(d));
  }

  private void removeLast_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThrows(NoSuchElementException.class, deque::removeLast);
  }

  @Test
  public void removeLast_whenPopulated() {
    full().forEach(d -> removeLast_whenPopulatedImpl(d));
  }

  private void removeLast_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var last = deque.peekLast();
    assertThat(deque.removeLast()).isSameInstanceAs(last);
    assertThat(deque.contains(last)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void removeLast_toEmpty() {
    full().forEach(d -> removeLast_toEmptyImpl(d));
  }

  private void removeLast_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    while (!deque.isEmpty()) {
      var value = deque.removeLast();
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeFirstOccurrence_notFound() {
    empty().forEach(d -> removeFirstOccurrence_notFoundImpl(d));
  }

  private void removeFirstOccurrence_notFoundImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.removeFirstOccurrence(new LinkedValue(0))).isFalse();
  }

  @Test
  public void removeFirstOccurrence_whenFound() {
    full().forEach(d -> removeFirstOccurrence_whenFoundImpl(d));
  }

  private void removeFirstOccurrence_whenFoundImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.removeFirstOccurrence(first)).isTrue();
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void removeFirstOccurrence_toEmpty() {
    full().forEach(d -> removeFirstOccurrence_toEmptyImpl(d));
  }

  private void removeFirstOccurrence_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    while (!deque.isEmpty()) {
      var value = deque.peekFirst();
      assertThat(deque.removeFirstOccurrence(value)).isTrue();
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeLastOccurrence_notFound() {
    empty().forEach(d -> removeLastOccurrence_notFoundImpl(d));
  }

  private void removeLastOccurrence_notFoundImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.removeLastOccurrence(new LinkedValue(0))).isFalse();
  }

  @Test
  public void removeLastOccurrence_whenFound() {
    full().forEach(d -> removeLastOccurrence_whenFoundImpl(d));
  }

  private void removeLastOccurrence_whenFoundImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.removeLastOccurrence(first)).isTrue();
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void removeLastOccurrence_toEmpty() {
    full().forEach(d -> removeLastOccurrence_toEmptyImpl(d));
  }

  private void removeLastOccurrence_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    while (!deque.isEmpty()) {
      var value = deque.peekFirst();
      assertThat(deque.removeLastOccurrence(value)).isTrue();
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeAll_withEmpty() {
    empty().forEach(d -> removeAll_withEmptyImpl(d));
  }

  private void removeAll_withEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.removeAll(List.of())).isFalse();
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void remove_withPopulated() {
    full().forEach(d -> remove_withPopulatedImpl(d));
  }

  private void remove_withPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var first = requireNonNull(deque.peekFirst());
    assertThat(deque.removeAll(List.of(first))).isTrue();
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void removeAll_toEmpty() {
    full().forEach(d -> removeAll_toEmptyImpl(d));
  }

  private void removeAll_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.removeAll(List.copyOf(deque))).isTrue();
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeAll_self() {
    full().forEach(d -> removeAll_selfImpl(d));
  }

  @SuppressWarnings("ModifyingCollectionWithItself")
  private void removeAll_selfImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.removeAll(deque)).isTrue();
    assertThat(deque).isExhaustivelyEmpty();
  }

  @Test
  public void removeAll_self_whenEmpty() {
    empty().forEach(d -> removeAll_self_whenEmptyImpl(d));
  }

  @SuppressWarnings("ModifyingCollectionWithItself")
  private void removeAll_self_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.removeAll(deque)).isFalse();
    assertThat(deque).isExhaustivelyEmpty();
  }

  /* --------------- Stack --------------- */

  @Test
  public void push_whenEmpty() {
    empty().forEach(d -> push_whenEmptyImpl(d));
  }

  @SuppressWarnings("DequeAddFirst")
  private void push_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(1);
    deque.push(value);
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isSameInstanceAs(value);
    assertThat(deque).hasSize(1);
  }

  @Test
  public void push_whenPopulated() {
    full().forEach(d -> push_whenPopulatedImpl(d));
  }

  @SuppressWarnings("DequeAddFirst")
  private void push_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var value = new LinkedValue(SIZE);
    deque.push(value);
    assertThat(deque.peekFirst()).isSameInstanceAs(value);
    assertThat(deque.peekLast()).isNotSameInstanceAs(value);
    assertThat(deque).hasSize(SIZE + 1);
  }

  @Test
  public void push_whenLinked() {
    full().forEach(d -> push_whenLinkedImpl(d));
  }

  private void push_whenLinkedImpl(LinkedDeque<LinkedValue> deque) {
    var value = requireNonNull(deque.peekFirst());
    assertThrows(IllegalArgumentException.class, () -> deque.push(value));
  }

  @Test
  public void pop_whenEmpty() {
    empty().forEach(d -> pop_whenEmptyImpl(d));
  }

  private void pop_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThrows(NoSuchElementException.class, deque::pop);
  }

  @Test
  public void pop_whenPopulated() {
    full().forEach(d -> pop_whenPopulatedImpl(d));
  }

  @SuppressWarnings("DequeRemoveFirst")
  private void pop_whenPopulatedImpl(LinkedDeque<LinkedValue> deque) {
    var first = deque.peekFirst();
    assertThat(deque.pop()).isSameInstanceAs(first);
    assertThat(deque.contains(first)).isFalse();
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void pop_toEmpty() {
    full().forEach(d -> pop_toEmptyImpl(d));
  }

  @SuppressWarnings("DequeRemoveFirst")
  private void pop_toEmptyImpl(LinkedDeque<LinkedValue> deque) {
    while (!deque.isEmpty()) {
      var value = deque.pop();
      assertThat(deque.contains(value)).isFalse();
    }
    assertThat(deque).isExhaustivelyEmpty();
  }

  /* --------------- Iterators --------------- */

  @Test
  public void iterator_noMoreElements() {
    empty().forEach(d -> iterator_noMoreElementsImpl(d));
  }

  private void iterator_noMoreElementsImpl(LinkedDeque<LinkedValue> deque) {
    var iterator = deque.iterator();
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void iterator_whenEmpty() {
    empty().forEach(d -> iterator_whenEmptyImpl(d));
  }

  private void iterator_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.iterator().hasNext()).isFalse();
    assertThat(deque.iterator().peek()).isNull();
  }

  @Test
  public void iterator_whenWarmed() {
    full().forEach(d -> iterator_whenWarmedImpl(d));
  }

  private void iterator_whenWarmedImpl(LinkedDeque<LinkedValue> deque) {
    var expected = new ArrayList<LinkedValue>();
    populate(expected);

    assertThat(deque.peekFirst()).isNotNull();
    assertThat(iteratorSize(deque.iterator())).isEqualTo(deque.size());
    assertThat(elementsEqual(deque.iterator(), expected.iterator())).isTrue();
  }

  @Test
  public void iterator_removal() {
    full().forEach(d -> iterator_removalImpl(d));
  }

  private void iterator_removalImpl(LinkedDeque<LinkedValue> deque) {
    var iterator = deque.iterator();
    var value = iterator.next();
    iterator.remove();

    int remaining = 0;
    while (iterator.hasNext()) {
      assertThat(iterator.next()).isNotSameInstanceAs(value);
      remaining++;
    }
    assertThat(remaining).isEqualTo(SIZE - 1);
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void iterator_removal_exception() {
    full().forEach(d -> iterator_removal_exceptionImpl(d));
  }

  private void iterator_removal_exceptionImpl(LinkedDeque<LinkedValue> deque) {
    var iterator = deque.iterator();
    iterator.next();
    iterator.remove();
    assertThrows(IllegalStateException.class, iterator::remove);
  }

  @Test
  public void descendingIterator_noMoreElements() {
    empty().forEach(d -> descendingIterator_noMoreElementsImpl(d));
  }

  private void descendingIterator_noMoreElementsImpl(LinkedDeque<LinkedValue> deque) {
    var iterator = deque.descendingIterator();
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void descendingIterator_whenEmpty() {
    empty().forEach(d -> descendingIterator_whenEmptyImpl(d));
  }

  private void descendingIterator_whenEmptyImpl(LinkedDeque<LinkedValue> deque) {
    assertThat(deque.descendingIterator().hasNext()).isFalse();
    assertThat(deque.descendingIterator().peek()).isNull();
  }

  @Test
  public void descendingIterator_whenWarmed() {
    full().forEach(d -> descendingIterator_whenWarmedImpl(d));
  }

  private void descendingIterator_whenWarmedImpl(LinkedDeque<LinkedValue> deque) {
    var expected = new ArrayList<LinkedValue>();
    populate(expected);
    Collections.reverse(expected);

    assertThat(deque.descendingIterator().peek()).isNotNull();
    assertThat(elementsEqual(deque.descendingIterator(), expected.iterator())).isTrue();
  }

  @Test
  public void descendingIterator_removal() {
    full().forEach(d -> descendingIterator_removalImpl(d));
  }

  private void descendingIterator_removalImpl(LinkedDeque<LinkedValue> deque) {
    var iterator = deque.descendingIterator();
    var value = iterator.next();
    iterator.remove();

    int remaining = 0;
    while (iterator.hasNext()) {
      assertThat(iterator.next()).isNotEqualTo(value);
      remaining++;
    }
    assertThat(remaining).isEqualTo(SIZE - 1);
    assertThat(deque).hasSize(SIZE - 1);
  }

  @Test
  public void concat() {
    full().forEach(d -> concatImpl(d));
  }

  private void concatImpl(LinkedDeque<LinkedValue> deque) {
    var expect = copyOfIterator(concatIterators(deque.iterator(), deque.descendingIterator()));
    Iterable<LinkedValue> actual =
        () -> PeekingIterator.concat(deque.iterator(), deque.descendingIterator());
    assertThat(actual).containsExactlyElementsIn(expect).inOrder();
  }

  @Test
  public void concat_peek() {
    full().forEach(d -> concat_peekImpl(d));
  }

  private void concat_peekImpl(LinkedDeque<LinkedValue> deque) {
    var iterator = PeekingIterator.concat(deque.iterator(), deque.iterator());
    while (iterator.hasNext()) {
      var expected = iterator.peek();
      assertThat(iterator.next()).isEqualTo(expected);
    }
    assertThat(iterator.peek()).isNull();
  }

  @Test
  public void concat_noMoreElements() {
    empty().forEach(d -> concat_noMoreElementsImpl(d));
  }

  private void concat_noMoreElementsImpl(LinkedDeque<LinkedValue> deque) {
    var iterator = PeekingIterator.concat(deque.iterator(), deque.iterator());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void comparing() {
    full().forEach(d -> comparingImpl(d));
  }

  @SuppressWarnings("SequencedCollectionGetFirst")
  private void comparingImpl(LinkedDeque<LinkedValue> deque) {
    var expect = copyOfIterator(concatIterators(deque.iterator(), deque.descendingIterator()));
    var actual =
        PeekingIterator.comparing(
            deque.iterator(), deque.descendingIterator(), comparator().reversed());
    assertThat(actual.peek()).isEqualTo(expect.get(0));
    assertThat(copyOfIterator(actual)).containsExactlyElementsIn(expect).inOrder();
  }

  @Test
  public void comparing_peek() {
    full().forEach(d -> comparing_peekImpl(d));
  }

  private void comparing_peekImpl(LinkedDeque<LinkedValue> deque) {
    var ascending =
        PeekingIterator.comparing(
            deque.descendingIterator(), deque.iterator(), comparator().reversed());
    var descending =
        PeekingIterator.comparing(deque.descendingIterator(), deque.iterator(), comparator());
    assertThat(ascending.peek()).isEqualTo(deque.peekFirst());
    assertThat(descending.peek()).isEqualTo(deque.peekLast());
  }

  @Test
  public void comparing_uneven() {
    full().forEach(d -> comparing_unevenImpl(d));
  }

  private void comparing_unevenImpl(LinkedDeque<LinkedValue> deque) {
    var empty = new AccessOrderDeque<LinkedValue>().iterator();
    var left = PeekingIterator.comparing(deque.iterator(), empty, comparator().reversed());
    var right = PeekingIterator.comparing(empty, deque.iterator(), comparator().reversed());

    assertThat(left.peek()).isEqualTo(deque.getFirst());
    assertThat(right.peek()).isEqualTo(deque.getFirst());
  }

  private static Comparator<LinkedValue> comparator() {
    return Comparator.comparingInt((LinkedValue v) -> v.value);
  }

  /* --------------- modCount + structural link/unlink --------------- */

  @Test
  @SuppressWarnings("CheckReturnValue")
  public void modCount_incrementedOnMutation() {
    var deque = new AccessOrderDeque<LinkedValue>();
    int initial = deque.modCount;

    deque.linkFirst(new LinkedValue(1));
    assertThat(deque.modCount).isEqualTo(initial + 1);

    deque.linkLast(new LinkedValue(2));
    assertThat(deque.modCount).isEqualTo(initial + 2);

    deque.unlinkFirst();
    assertThat(deque.modCount).isEqualTo(initial + 3);

    deque.unlinkLast();
    assertThat(deque.modCount).isEqualTo(initial + 4);

    deque.linkFirst(new LinkedValue(3));
    var peeked = deque.peekFirst();
    assertThat(peeked).isNotNull();
    deque.unlink(peeked);
    assertThat(deque.modCount).isEqualTo(initial + 6);

    deque.linkFirst(new LinkedValue(4));
    deque.clear();
    assertThat(deque.modCount).isEqualTo(initial + 8);
  }

  @Test
  public void linkFirst_setsPreviousLink() {
    var deque = new AccessOrderDeque<LinkedValue>();
    var first = new LinkedValue(1);
    deque.linkFirst(first);
    var newFirst = new LinkedValue(2);
    deque.linkFirst(newFirst);
    assertThat(first.getPreviousInAccessOrder()).isEqualTo(newFirst);
    assertThat(newFirst.getNextInAccessOrder()).isEqualTo(first);
  }

  @Test
  public void unlink_clearsPointers() {
    var deque = new AccessOrderDeque<LinkedValue>();
    var a = new LinkedValue(1);
    var b = new LinkedValue(2);
    var c = new LinkedValue(3);
    deque.addLast(a);
    deque.addLast(b);
    deque.addLast(c);

    deque.unlink(b);
    assertThat(b.getPreviousInAccessOrder()).isNull();
    assertThat(b.getNextInAccessOrder()).isNull();
    assertThat(a.getNextInAccessOrder()).isEqualTo(c);
    assertThat(c.getPreviousInAccessOrder()).isEqualTo(a);
  }

  @Test
  public void clear_clearsAllLinks() {
    var deque = new AccessOrderDeque<LinkedValue>();
    var a = new LinkedValue(1);
    var b = new LinkedValue(2);
    deque.addLast(a);
    deque.addLast(b);

    deque.clear();
    assertThat(a.getPreviousInAccessOrder()).isNull();
    assertThat(a.getNextInAccessOrder()).isNull();
    assertThat(b.getPreviousInAccessOrder()).isNull();
    assertThat(b.getNextInAccessOrder()).isNull();
  }

  /* --------------- Deque providers --------------- */

  static Stream<LinkedDeque<LinkedValue>> empty() {
    return Stream.of(new AccessOrderDeque<>(), new WriteOrderDeque<>());
  }

  static Stream<LinkedDeque<LinkedValue>> full() {
    var accessOrder = new AccessOrderDeque<LinkedValue>();
    var writeOrder = new WriteOrderDeque<LinkedValue>();
    populate(accessOrder);
    populate(writeOrder);
    return Stream.of(accessOrder, writeOrder);
  }

  private static void populate(Collection<LinkedValue> collection) {
    for (int i = 0; i < SIZE; i++) {
      collection.add(new LinkedValue(i));
    }
  }

  private static final class LinkedValue
      implements AccessOrder<LinkedValue>, WriteOrder<LinkedValue> {
    private final int value;

    private LinkedValue prev;
    private LinkedValue next;

    LinkedValue(int value) {
      this.value = value;
    }

    @Override
    public LinkedValue getPreviousInAccessOrder() {
      return prev;
    }

    @Override
    public void setPreviousInAccessOrder(LinkedValue prev) {
      this.prev = prev;
    }

    @Override
    public LinkedValue getNextInAccessOrder() {
      return next;
    }

    @Override
    public void setNextInAccessOrder(LinkedValue next) {
      this.next = next;
    }

    @Override
    public LinkedValue getPreviousInWriteOrder() {
      return prev;
    }

    @Override
    public void setPreviousInWriteOrder(LinkedValue prev) {
      this.prev = prev;
    }

    @Override
    public LinkedValue getNextInWriteOrder() {
      return next;
    }

    @Override
    public void setNextInWriteOrder(LinkedValue next) {
      this.next = next;
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof LinkedValue) && (value == ((LinkedValue) o).value);
    }

    @Override
    public int hashCode() {
      return value;
    }

    @Override
    public String toString() {
      return "LinkedValue{value="
          + value
          + ", prev="
          + (prev == null ? null : prev.value)
          + ", next="
          + (next == null ? null : next.value)
          + "}";
    }
  }

  private static <T> T getAt(Iterable<T> iterable, int index) {
    int i = 0;
    for (T item : iterable) {
      if (i++ == index) {
        return item;
      }
    }
    throw new IndexOutOfBoundsException("Index: " + index);
  }

  private static int iteratorSize(Iterator<?> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    return count;
  }

  private static <T> List<T> copyOfIterator(Iterator<T> iterator) {
    List<T> list = new ArrayList<>();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return Collections.unmodifiableList(list);
  }

  private static <T> Iterator<T> concatIterators(Iterator<T> first, Iterator<T> second) {
    List<T> combined = new ArrayList<>();
    while (first.hasNext()) {
      combined.add(first.next());
    }
    while (second.hasNext()) {
      combined.add(second.next());
    }
    return combined.iterator();
  }

  private static boolean elementsEqual(Iterator<?> it1, Iterator<?> it2) {
    while (it1.hasNext()) {
      if (!it2.hasNext() || !Objects.equals(it1.next(), it2.next())) {
        return false;
      }
    }
    return !it2.hasNext();
  }
}
