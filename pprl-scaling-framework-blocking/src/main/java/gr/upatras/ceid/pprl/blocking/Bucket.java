package gr.upatras.ceid.pprl.blocking;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Bucket
 */
public class Bucket extends Node{

    private String name;
    private List<BitSet> bitSets;

    public Bucket(final Node n) {
        super(n);
        name = "";
        int i=0;
        for(int sb : super.getSplitBits()) {
            if(i!=0) name += " , ";
            name += sb + "=" + ((super.getSplitBitValues().get(i))?1:0);
            i++;
        }
        if(n instanceof MBT.SBTNode) {
            bitSets = Arrays.asList(((MBT.SBTNode) n).getBitSets());
        }
    }

    public String getName() {
        return name;
    }

    public List<BitSet> getBitSets() {
        return bitSets;
    }

    @Override
    public String toString() {
        return "Bucket{" +
                super.toString() +
                ", name='" + name + '\'' +
                ", bitSets.size()=" + bitSets.size() +
                '}';
    }
}

