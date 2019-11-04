package com.gslab.pepper.input;

import java.util.HashSet;
import java.util.Set;

/**
 * The CustomFunctions allows users to write custom functions and then it can be used in template.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class CustomFunctions {

    private static final String ALPHABET = "abcedefghijklmnopqrwxyzABCDEFGHIJKLMNOPQRWXYZ0123456789";

    public static String RANDOM_VALID_MESSAGES(int minSize, int maxSize, int msgMaxLength) {
        int size = FieldDataFunctions.RANDOM_INT_RANGE(minSize, maxSize);

        Set<Integer> used = new HashSet<>(size);
        StringBuilder sb = new StringBuilder().append('[');

        while (used.size() < size) {
            int id = FieldDataFunctions.RANDOM_INT_RANGE(0, Integer.MAX_VALUE);
            if (used.add(id)) {
                int length = FieldDataFunctions.RANDOM_INT_RANGE(1, msgMaxLength);
                String payload = FieldDataFunctions.RANDOM_ALPHA_NUMERIC(ALPHABET, length);
                if (used.size() > 1) {
                    sb.append(", ");
                }
                sb.append('{').append("\"messageId\": ").append(id).append(", \"payload\": \"").append(payload).append("\"}");
            }
        }


        return sb.append(']').toString();
    }

}
