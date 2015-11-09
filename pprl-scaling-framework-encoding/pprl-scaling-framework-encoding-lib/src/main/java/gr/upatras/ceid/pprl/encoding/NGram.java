package gr.upatras.ceid.pprl.encoding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class NGram {

    public static ArrayList<String> getGrams(String word, int n) {
        ArrayList<String> ngrams = new ArrayList();
        int len = word.length();
        for(int i = 0; i < len; i++) {
                if(i > (n - 2)) {
                        String ng = "";
                        for(int j = n-1; j >= 0; j--) {
                                ng = ng + word.charAt(i-j);
                        }
                        ngrams.add(ng);
                }
        }
        return ngrams;
}
    
    
   public static HashMap<String,Integer> getHashBigrams() {
        String s1 = "abcdefghijklmnopqrstuvwxyz";
        int a = 0;
        HashMap<String,Integer>  map = new HashMap();
        for (int i = 0; i < s1.length(); i++) {
            char c1 = s1.charAt(i);
            for (int j = 0; j < s1.length(); j++) {
                char c2 = s1.charAt(j);
                String s2 = new String(new char[]{c1, c2});
                List<String> al = getGrams(s2, 2);
                for (String s : al) {                    
                    map.put(s, a) ;
                    a++;
                }
                
                /* String s3 = new String(new char[]{c2, c1});
                List<String> al1 = getGrams(s3, 2);
                for (String s : al1) {
                    a++;
                    map.put(s, a + "");
                }*/
            }
        }
        //System.out.println(" a="+a);
        return map;      
    }

    public static void main(String[] args) {
        List<String> al=getGrams("john",2);
        HashMap<String,Integer> map=getHashBigrams();
        Iterator it = map.entrySet().iterator();
        /*while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            System.out.println(pairs.getKey() + " = " + pairs.getValue());
        }*/
        int[] bigram=new int[633];
        for (String s: al) {
            bigram[map.get(s)]=1;   
            System.out.println(map.get(s)+" "+s);
        }
    }
}
