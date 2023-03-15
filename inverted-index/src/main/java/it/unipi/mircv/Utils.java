package it.unipi.mircv;

public class Utils {


    public static String leftpad(String text, int length) {
        return String.format("%" + length + "." + length + "s", text);
    }

    public static String rightpad(String text, int length) {
        return String.format("%-" + length + "." + length + "s", text);
    }

}
