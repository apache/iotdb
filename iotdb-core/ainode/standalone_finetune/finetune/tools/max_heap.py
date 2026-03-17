import heapq


class MaxHeap:
    def __init__(self):
        self.heap = []

    def push(self, index, weight):
        heapq.heappush(self.heap, (-weight, index))

    def pop(self):
        if not self.is_empty():
            neg_weight, index = heapq.heappop(self.heap)
            return index, -neg_weight
        return None

    def peek(self):
        if not self.is_empty():
            neg_weight, index = self.heap[0]
            return index, -neg_weight
        return None

    def is_empty(self):
        return len(self.heap) == 0

    def size(self):
        return len(self.heap)

    def __str__(self):
        return str([(idx, -w) for w, idx in self.heap])
