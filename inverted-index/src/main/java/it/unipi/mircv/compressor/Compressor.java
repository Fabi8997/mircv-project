package it.unipi.mircv.compressor;
import java.util.ArrayList;
import static it.unipi.mircv.utils.Utils.splitsLog128;

/**
 *Class containing the methods to compress/decompress a list of integers using the Variable Byte Encoding.
 */
public class Compressor {

    /**
     * Compress the given number using the Variable Byte Encoding.
     * @param number Number to be compressed
     * @return Array of bytes containing the code of the number
     */
    public static byte[] variableByteEncodeNumber(int number){

        //Retrieve the number of splits required to encode the number
        int numberOfBytes = splitsLog128(number);

        //Array to hold the encoded bytes
        byte[] bytes = new byte[numberOfBytes];

        //Write the number representation in big-endian order from the MSByte to the LSByte
        for(int i = numberOfBytes - 1; i >= 0; i--){

            //Prepend of the reminder of the division by 128 (retrieve the 7 LSB)
            byte b = (byte) (number % 128);
            bytes[i] = b;

            //Shift right the number by 7 position
            number /= 128;
        }

        //Set the control bit of the last byte to 1, to indicate that it is the last byte
        bytes[numberOfBytes - 1] += 128;

        //Return the encoded number
        return bytes;
    }

    /**
     * Compress the given list of numbers using the Variable Byte Encoding; it will return a list of bytes that is
     * the concatenation of the numbers' codes.
     * @param numbers Numbers to be compressed.
     * @return Array of bytes containing the compressed numbers.
     */
    public static byte[] variableByteEncode(ArrayList<Integer> numbers){

        //Array to hold the bytes of the encoded numbers
        ArrayList<Byte> bytes = new ArrayList<>();

        //For each number in the list
        for (Integer number : numbers) {

            //Encode each number and add it to the end of the array
            for(byte b : variableByteEncodeNumber(number)){
                bytes.add(b);
            }
        }

        //Array used to convert the arrayList into a byte array
        byte[] result = new byte[bytes.size()];

        //For each byte in the arrayList put it in the array of bytes
        for(int i = 0; i < bytes.size(); i++){
            result[i] = bytes.get(i);
        }

        //Return the array of bytes
        return result;
    }


    // TODO: 29/03/2023 Decode using VB

    /**
     * Decode the given array of bytes that contains a Variable Byte Encoding of a list of integers, returning the
     * corresponding list of integers.
     * @param bytes Compressed list of integers.
     * @return Decompressed list of integers.
     */
    public static ArrayList<Integer> variableByteDecode(byte[] bytes){

        //Array to hold the decoded numbers
        ArrayList<Integer> numbers = new ArrayList<>();

        // TODO: 29/03/2023 Body of decoding function

        return numbers;
    }
}
