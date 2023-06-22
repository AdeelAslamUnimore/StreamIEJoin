package com.baselinealgorithm;

import java.util.List;

public interface InMemoryIndex  {
    public int size();

    public int height();

    public List<Integer> get(int key);

    public void put(int key, int value);

    public void remove(int key);

    public void remove(int key, int value);
}
