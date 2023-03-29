package it.unipi.mircv.utils;

/**
 * A utility class containing methods for generic purposes.
 */
public class Utils {


    /**
     * Given a string and a length, return a string on which are added whitespaces from the left to reach the length.
     * @param text String to be processed
     * @param length Length of the final string
     * @return String of length 'length'
     */
    public static String leftpad(String text, int length) {
        return String.format("%" + length + "." + length + "s", text);
    }

    public static String rightpad(String text, int length) {
        return String.format("%-" + length + "." + length + "s", text);
    }

    /**
     * Compute the number of splits of the given number needed to encode it using variable-length encoding.
     * It computes the base 128 logarithm of the number and add 1 to it, obtaining the number of groups of
     * 7 bits that composes the number's representation.
     * @param number Number of which compute the splits.
     * @return Returns the number of splits.
     */
    public static int splitsLog128(int number){
        return (int)(Math.floor(Math.log(number) / Math.log(128)) + 1);
    }
}
