package com.invertedindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Decompressor {

    Decompressor() {

    }

    Map<Integer, Integer[]> vbyteDecode(byte[] bytes) {
        Map<Integer, Integer[]> decodedArray = new HashMap<Integer, Integer[]>();
        Integer[] data = new Integer[100000];
        int n = 0, invertedListSize = 0;

        for (byte b : bytes) {
            if ((b & 0xff) < 128) {
                n = 128 * n + b;
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                data[invertedListSize++] = num;
                n = 0;
            }
        }
        decodedArray.put(invertedListSize, data);
        return decodedArray;
    }

    ArrayList<Integer> deltaDecode(Integer[] inputSequence, int size) {

        int j, i = 0, prev_docnum = 0, tf_val;
        ArrayList<Integer> inv_list = new ArrayList<Integer>();

        while (i < size) {
            inputSequence[i] += prev_docnum;
            prev_docnum = inputSequence[i++];

            tf_val = inputSequence[i++];
            System.out.println(tf_val);


            j = i;
            while (j < i + tf_val) {
                if (j == i) {
                    j++;
                } else if (j >= size) {
                    System.out.println("Size is " + size + " Error here at j=" + j + " with i=" + i + " and i+tf=" + (i + tf_val));
                    break;
                } else {
                    inputSequence[j] += inputSequence[j - 1];
                    ++j;
                }
            }
            i = j;
        }

        for (int x = 0; x < size; ++x)
            inv_list.add(inputSequence[x]);
        return inv_list;


    }
}
