package gr.upatras.ceid.pprl.blocking;

import java.util.ArrayList;
import java.util.List;

/**
 * Tree node
 */
public class Node {

    private static int ID=0;
    private int id;
    private int count;
    private List<Integer> splitBits = new ArrayList<Integer>();
    private List<Boolean> splitBitValues = new ArrayList<Boolean>();


    public Node() {
        id = ID;
        ID++;
    }

    public Node(int count) {
        this.count = count;
    }

    public Node(int id, int count) {
        this.id = id;
        this.count = count;
    }

    public Node(int id, int count, List<Integer> splitBits, List<Boolean> splitBitValues) {
        this.id = id;
        this.count = count;
        this.splitBits = splitBits;
        this.splitBitValues = splitBitValues;
    }

    public Node(int count, List<Integer> splitBits, List<Boolean> splitBitValues) {
        this.count = count;
        this.splitBits = splitBits;
        this.splitBitValues = splitBitValues;
    }

    public Node(final Node n){
        this.id = n.getId();
        this.count = n.getCount();
        this.splitBits = n.getSplitBits();
        this.splitBitValues = n.getSplitBitValues();
    }

    public int getId() {
        return id;
    }

    public int getCount() {
        return count;
    }

    public List<Integer> getSplitBits() {
        return splitBits;
    }

    public List<Boolean> getSplitBitValues() {
        return splitBitValues;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", count=" + count +
                ", splitBits=" + splitBits +
                ", splitBitValues=" + splitBitValues +
                '}';
    }
}
