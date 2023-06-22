package com.baselinealgorithm.chainindexrbst;

import com.baselinealgorithm.InMemoryIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import static com.google.common.collect.Lists.newLinkedList;

public class RedBlackBST implements InMemoryIndex {
    private static final boolean RED = true;
    private static final boolean BLACK = false;

    public Node root; // root of the BST
//    public class Node
//    {
//        private int key; // key
//        private List<Integer> vals; // associated data
//        private Node left, right; // links to left and right subtrees
//        private boolean color; // color of parent link
//        private int size; // subtree count
//
//        public Node(int key, int val, boolean color, int size) {
//            this.key = key;
//            vals = newLinkedList();
//            vals.add(val);
//            this.color = color;
//            this.size = size;
//        }
//    }

    private boolean isRed(Node x) {
        if (x == null)
            return false;
        return (x.color == RED);
    }

    // number of node in subtree rooted at x; 0 if x is null
    private int size(Node x) {
        if (x == null)
            return 0;
        return x.size;
    }

    // return number of key-value pairs in this symbol table
    @Override
    public int size() {
        return size(root);
    }

    // is this symbol table empty?
    public boolean isEmpty() {
        return root == null;
    }

    // value associated with the given key; null if no such key
    public List<Integer> get(int key) {
        return get(root, key);
    }

    // value associated with the given key in subtree rooted at x; null if no
    // such key
    private List<Integer> get(Node x, int key) {
        while (x != null) {
            int cmp = Integer.compare(key, x.key);
            // int cmp = key.compareTo(x.key);
            if (cmp < 0)
                x = x.left;
            else if (cmp > 0)
                x = x.right;
            else
                return x.vals;
        }

        return null;
    }

    // is there a key-value pair with the given key?
    public boolean contains(int key) {
        return (get(key) != null);
    }

    // is there a key-value pair with the given key in the subtree rooted at x?
    private boolean contains(Node x, int key) {
        return (get(x, key) != null);
    }

    // insert the key-value pair; overwrite the old value with the new value
    // if the key is already present
    public void put(int key, int val) {
        root = put(root, key, val);
        root.color = BLACK;
        assert check();
    }

    private Node put(Node h, int key, int val) {
        if (h == null)
            return new Node(key, val, RED, 1);
        int cmp = Integer.compare(key, h.key);
        // int cmp = key.compareTo(h.key);
        if (cmp < 0)
            h.left = put(h.left, key, val);
        else if (cmp > 0)
            h.right = put(h.right, key, val);
        else
            h.vals.add(val);

        // fix-up any right-leaning links
        if (isRed(h.right) && !isRed(h.left))
            h = rotateLeft(h);
        if (isRed(h.left) && isRed(h.left.left))
            h = rotateRight(h);
        if (isRed(h.left) && isRed(h.right))
            flipColors(h);
        h.size = size(h.left) + size(h.right) + 1;

        return h;
    }

    public void removeMin() {
        if (isEmpty())
            throw new NoSuchElementException("BST underflow");

        // if both children of root are black, set root to red
        if (!isRed(root.left) && !isRed(root.right))
            root.color = RED;

        root = removeMin(root);
        if (!isEmpty())
            root.color = BLACK;
        assert check();
    }

    private Node removeMin(Node h) {
        if (h.left == null)
            return null;

        if (!isRed(h.left) && !isRed(h.left.left))
            h = moveRedLeft(h);

        h.left = removeMin(h.left);
        return balance(h);
    }

    // remove the key-value pair with the maximum key
    public void removeMax() {
        if (isEmpty())
            throw new NoSuchElementException("BST underflow");

        // if both children of root are black, set root to red
        if (!isRed(root.left) && !isRed(root.right))
            root.color = RED;

        root = removeMax(root);
        if (!isEmpty())
            root.color = BLACK;
        assert check();
    }

    // remove the key-value pair with the maximum key rooted at h
    private Node removeMax(Node h) {
        if (isRed(h.left))
            h = rotateRight(h);

        if (h.right == null)
            return null;

        if (!isRed(h.right) && !isRed(h.right.left))
            h = moveRedRight(h);

        h.right = removeMax(h.right);

        return balance(h);
    }

    // remove the key-value pair with the given key
    public void remove(int key) {
        if (!contains(key)) {
            System.err.println("symbol table does not contain " + key);
            return;
        }

        // if both children of root are black, set root to red
        if (!isRed(root.left) && !isRed(root.right))
            root.color = RED;

        root = remove(root, key);
        if (!isEmpty())
            root.color = BLACK;
        assert check();
    }

    // remove the key-value pair with the given key rooted at h
    private Node remove(Node h, int key) {
        assert contains(h, key);
        //int cmp= Integer.compare(key, x.key);
        if (Integer.compare(key, h.key) < 0) {
            if (!isRed(h.left) && !isRed(h.left.left))
                h = moveRedLeft(h);
            h.left = remove(h.left, key);
        } else {
            if (isRed(h.left))
                h = rotateRight(h);
            if (Integer.compare(key, h.key) == 0 && (h.right == null))
                return null;
            if (!isRed(h.right) && !isRed(h.right.left))
                h = moveRedRight(h);
            if (Integer.compare(key, h.key) == 0) {
                Node x = min(h.right);
                h.key = x.key;
                h.vals = x.vals;
                h.right = removeMin(h.right);
            } else
                h.right = remove(h.right, key);
        }
        return balance(h);
    }

    public void remove(int key, int val) {
        // if both children of root are black, set root to red
        if (!isRed(root.left) && !isRed(root.right)) {
            root.color = RED;
        }

        root = remove(root, key, val);

        if (!isEmpty()) {
            root.color = BLACK;
        }
    }

    private Node remove(Node h, int key, int val) {
        if (Integer.compare(key, h.key) < 0) {
            if (!isRed(h.left) && !isRed(h.left.left)) {
                h = moveRedLeft(h);
            }
            h.left = remove(h.left, key, val);
        } else {
            if (isRed(h.left)) {
                h = rotateRight(h);
            }

            if (Integer.compare(key, h.key) == 0 && (h.right == null)) {
                h.vals.remove(val);

                if (h.vals.isEmpty()) {
                    return null;
                } else {
                    return h;
                }
            }

            if (!isRed(h.right) && !isRed(h.right.left)) {
                h = moveRedRight(h);
            }

            if (Integer.compare(key, h.key) == 0) {
                h.vals.remove(val);

                if (h.vals.isEmpty()) {
                    Node x = min(h.right);
                    h.key = x.key;
                    h.vals = x.vals;
                    h.right = removeMin(h.right);
                }
            } else {
                h.right = remove(h.right, key, val);
            }
        }

        return balance(h);
    }

    // make a left-leaning link lean to the right
    private Node rotateRight(Node h) {
        assert (h != null) && isRed(h.left);
        Node x = h.left;
        h.left = x.right;
        x.right = h;
        x.color = x.right.color;
        x.right.color = RED;
        x.size = h.size;
        h.size = size(h.left) + size(h.right) + 1;
        return x;
    }

    // make a right-leaning link lean to the left
    private Node rotateLeft(Node h) {
        assert (h != null) && isRed(h.right);
        Node x = h.right;
        h.right = x.left;
        x.left = h;
        x.color = x.left.color;
        x.left.color = RED;
        x.size = h.size;
        h.size = size(h.left) + size(h.right) + 1;
        return x;
    }

    // flip the colors of a node and its two children
    private void flipColors(Node h) {
        // h must have opposite color of its two children
        assert (h != null) && (h.left != null) && (h.right != null);
        assert (!isRed(h) && isRed(h.left) && isRed(h.right))
                || (isRed(h) && !isRed(h.left) && !isRed(h.right));
        h.color = !h.color;
        h.left.color = !h.left.color;
        h.right.color = !h.right.color;
    }

    // Assuming that h is red and both h.left and h.left.left
    // are black, make h.left or one of its children red.
    private Node moveRedLeft(Node h) {
        assert (h != null);
        assert isRed(h) && !isRed(h.left) && !isRed(h.left.left);

        flipColors(h);
        if (isRed(h.right.left)) {
            h.right = rotateRight(h.right);
            h = rotateLeft(h);
        }
        return h;
    }

    // Assuming that h is red and both h.right and h.right.left
    // are black, make h.right or one of its children red.
    private Node moveRedRight(Node h) {
        assert (h != null);
        assert isRed(h) && !isRed(h.right) && !isRed(h.right.left);
        flipColors(h);
        if (isRed(h.left.left)) {
            h = rotateRight(h);
        }
        return h;
    }

    // restore red-black tree invariant
    private Node balance(Node h) {
        assert (h != null);

        if (isRed(h.right))
            h = rotateLeft(h);
        if (isRed(h.left) && isRed(h.left.left))
            h = rotateRight(h);
        if (isRed(h.left) && isRed(h.right))
            flipColors(h);

        h.size = size(h.left) + size(h.right) + 1;
        return h;
    }

    // height of tree; 0 if empty
    public int height() {
        return height(root);
    }

    private int height(Node x) {
        if (x == null)
            return 0;
        return 1 + Math.max(height(x.left), height(x.right));
    }

    // the smallest key; null if no such key
    public int min() {
        if (isEmpty())
            return 0;
        return min(root).key;
    }

    // the smallest key in subtree rooted at x; null if no such key
    private Node min(Node x) {
        assert x != null;
        if (x.left == null)
            return x;
        else
            return min(x.left);
    }

    // the largest key; null if no such key
    public int max() {
        if (isEmpty())
            return 0;
        return max(root).key;
    }

    // the largest key in the subtree rooted at x; null if no such key
    private Node max(Node x) {
        assert x != null;
        if (x.right == null)
            return x;
        else
            return max(x.right);
    }

    // the largest key less than or equal to the given key
    public int floor(int key) {
        Node x = floor(root, key);
        if (x == null)
            return 0;
        else
            return x.key;
    }

    // the largest key in the subtree rooted at x less than or equal to the
    // given key
    private Node floor(Node x, int key) {
        if (x == null)
            return null;

        int cmp = Integer.compare(key, x.key);//compareTo(x.key);
        if (cmp == 0)
            return x;
        if (cmp < 0)
            return floor(x.left, key);
        Node t = floor(x.right, key);
        if (t != null)
            return t;
        else
            return x;
    }

    // the smallest key greater than or equal to the given key
    public int ceiling(int key) {
        Node x = ceiling(root, key);
        if (x == null)
            return 0;
        else
            return x.key;
    }

    // the smallest key in the subtree rooted at x greater than or equal to the
    // given key
    private Node ceiling(Node x, int key) {
        if (x == null)
            return null;
        int cmp = Integer.compare(key, x.key);//key.compareTo(x.key);
        if (cmp == 0)
            return x;
        if (cmp > 0)
            return ceiling(x.right, key);
        Node t = ceiling(x.left, key);
        if (t != null)
            return t;
        else
            return x;
    }

    // the key of rank k
    public int select(int k) {
        if (k < 0 || k >= size())
            return 0;
        Node x = select(root, k);
        return x.key;
    }

    // the key of rank k in the subtree rooted at x
    private Node select(Node x, int k) {
        assert x != null;
        assert k >= 0 && k < size(x);
        int t = size(x.left);
        if (t > k)
            return select(x.left, k);
        else if (t < k)
            return select(x.right, k - t - 1);
        else
            return x;
    }

    // number of keys less than key
    public int rank(int key) {
        return rank(key, root);
    }

    // number of keys less than key in the subtree rooted at x
    private int rank(int key, Node x) {
        if (x == null)
            return 0;

        int cmp = Integer.compare(key, x.key);
        if (cmp < 0)
            return rank(key, x.left);
        else if (cmp > 0)
            return 1 + size(x.left) + rank(key, x.right);
        else
            return size(x.left);
    }

    // number of keys greater than key
    public int countGreaterThan(int key) {
        return countGreaterThan(key, root);
    }

    // number of keys greater than key in the subtree rooted at x
    private int countGreaterThan(int key, Node x) {
        if (x == null)
            return 0;
        int cmp = Integer.compare(key, x.key);
        if (cmp < 0)
            return 1 + size(x.right) + countGreaterThan(key, x.left);
        else if (cmp > 0)
            return countGreaterThan(key, x.right);
        else
            return size(x.right);
    }


    public List<Node> getNodesGreaterThan(int key) {
        List<Node> result = new ArrayList<>();
        getNodesGreaterThan(key, root, result);
        return result;
    }

    // Helper method to retrieve nodes with keys greater than key in the subtree rooted at x
    private void getNodesGreaterThan(int key, Node x, List<Node> result) {
        if (x == null)
            return;
        int cmp = Integer.compare(key, x.key);
        if (cmp < 0) {
            result.add(x);
            getNodesGreaterThan(key, x.right, result);
            getNodesGreaterThan(key, x.left, result);
        } else {
            getNodesGreaterThan(key, x.right, result);
        }
    }

    public List<Node> lessThanKey(int key) {

        List<Node> result = new ArrayList<>();
        lessThanKey(key, root, result);
        return result;

//        return lessThanKey(key, root);
    }

    // number of keys less than key in the subtree rooted at x
    private void lessThanKey(int key, Node x, List<Node> results) {
        if (x == null)
            return;
        int cmp = Integer.compare(key, x.key);
        if (cmp > 0) {
            results.add(x);
            lessThanKey(key, x.left, results);
            lessThanKey(key, x.right, results);
        } else if (cmp < 0) {
            lessThanKey(key, x.left, results);
        } else {
            lessThanKey(key, x.left, results);
            lessThanKey(key, x.right, results);
        }
}


    // all of the keys, as an Iterable
    public Iterable<Integer> keys() {
        return keys(min(), max());
    }

    // the keys between lo and hi, as an Iterable
    public Iterable<Integer> keys(int lo, int hi) {
        Queue<Integer> queue = newLinkedList();
        keys(root, queue, lo, hi);
        return queue;
    }

    // add the keys between lo and hi in the subtree rooted at x
    // to the queue
    private void keys(Node x, Queue<Integer> queue, int lo, int hi) {
        if (x == null)
            return;
        int cmplo = Integer.compare(lo,x.key);
        int cmphi =  Integer.compare(hi,x.key);
        if (cmplo < 0)
            keys(x.left, queue, lo, hi);
        if (cmplo <= 0 && cmphi >= 0)
            queue.offer(x.key);
        if (cmphi > 0)
            keys(x.right, queue, lo, hi);
    }

    // number keys between lo and hi
    public int size(int lo, int hi) {

        if (Integer.compare(lo,hi) > 0)
            return 0;
        if (contains(hi))
            return rank(hi) - rank(lo) + 1;
        else
            return rank(hi) - rank(lo);
    }

    private boolean check() {
        if (!isBST())
            System.out.println("Not in symmetric order");
        if (!isSizeConsistent())
            System.out.println("Subtree counts not consistent");
        if (!isRankConsistent())
            System.out.println("Ranks not consistent");
        if (!is23())
            System.out.println("Not a 2-3 tree");
        if (!isBalanced())
            System.out.println("Not balanced");
        return isBST() && isSizeConsistent() && isRankConsistent() && is23()
                && isBalanced();
    }

    // does this binary tree satisfy symmetric order?
    // Note: this test also ensures that data structure is a binary tree since
    // order is strict
    private boolean isBST() {
        return isBST(root, 0, 0);
    }

    // is the tree rooted at x a BST with all keys strictly between min and max
    // (if min or max is null, treat as empty constraint)
    // Credit: Bob Dondero's elegant solution
    private boolean isBST(Node x, int min, int max) {
        if (x == null)
            return true;
       // Integer.compare(x.key,min)
        if (min != 0 &&  Integer.compare(x.key,min) <= 0)
            return false;
        if (max != 0 && Integer.compare(x.key,max) >= 0)
            return false;
        return isBST(x.left, min, x.key) && isBST(x.right, x.key, max);
    }

    // are the size fields correct?
    private boolean isSizeConsistent() {
        return isSizeConsistent(root);
    }

    private boolean isSizeConsistent(Node x) {
        if (x == null)
            return true;
        if (x.size != size(x.left) + size(x.right) + 1)
            return false;
        return isSizeConsistent(x.left) && isSizeConsistent(x.right);
    }

    // check that ranks are consistent
    private boolean isRankConsistent() {
        for (int i = 0; i < size(); i++)
            if (i != rank(select(i)))
                return false;
        for (int key : keys())
            if (Integer.compare(key, select(rank(key))) != 0)
                return false;
        return true;
    }

    // Does the tree have no red right links, and at most one (left)
    // red links in a row on any path?
    private boolean is23() {
        return is23(root);
    }

    private boolean is23(Node x) {
        if (x == null)
            return true;
        if (isRed(x.right))
            return false;
        if (x != root && isRed(x) && isRed(x.left))
            return false;
        return is23(x.left) && is23(x.right);
    }

    // do all paths from root to leaf have same number of black edges?
    private boolean isBalanced() {
        int black = 0; // number of black links on path from root to min
        Node x = root;
        while (x != null) {
            if (!isRed(x))
                black++;
            x = x.left;
        }
        return isBalanced(root, black);
    }

    // does every path from the root to a leaf have the given number of black
    // links?
    private boolean isBalanced(Node x, int black) {
        if (x == null)
            return black == 0;
        if (!isRed(x))
            black--;
        return isBalanced(x.left, black) && isBalanced(x.right, black);
    }

    public void printInOrder() {
        printInOrder(root);
        System.out.println();
    }

    private void printInOrder(Node h) {
        if (h == null) {
            return;
        }

        printInOrder(h.left);

        for (int val : h.vals) {
            System.out.print(val + " ");
        }

        printInOrder(h.right);
    }
}
