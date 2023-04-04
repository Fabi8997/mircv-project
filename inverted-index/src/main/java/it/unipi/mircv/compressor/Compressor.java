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
    public static byte[] variableByteEncodeNumber(long number){

        //If the number is 0, we return directly the code in VB of 0, otherwise the algorithm doesn't work
        //In particular the log doesn't exist at 0, log(x) exists for x > 0
        if(number == 0){
            return new byte[]{(byte) 0x80};//In practice this case shouldn't happen, since a frequency is always > 0
        }

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
    public static byte[] variableByteEncodeLong(ArrayList<Long> numbers){

        //Array to hold the bytes of the encoded numbers
        ArrayList<Byte> bytes = new ArrayList<>();

        //For each number in the list
        for (Long number : numbers) {

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

    public static byte[] variableByteEncodeInt(ArrayList<Integer> numbers){

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


    /**
     * Decode the given array of bytes that contains a Variable Byte Encoding of a list of integers, returning the
     * corresponding list of integers.
     * @param bytes Compressed list of integers.
     * @return Decompressed list of integers.
     */
    public static ArrayList<Integer> variableByteDecode(byte[] bytes){

        //Array to hold the decoded numbers
        ArrayList<Integer> numbers = new ArrayList<>();

        //Accumulator for the current decoded number
        int number = 0;

        //For each byte in the array
        for (byte aByte : bytes) {

            //We use the mask 0x80 = 1000 0000, to check if the MSB of the byte is 1
            if ((aByte & 0x80) == 0x00) {
                //The MSB is 0, then we're not at the end of the sequence of bytes of the code
                number = number * 128 + aByte;
            } else {
                //The MSB is 1, then we're at the end

                //Add to the accumulator number*128 + the integer value in aByte discarding the 1 in the MSB
                number = number * 128 + (aByte &  0x7F);

                //Add the decoded number to the list of numbers
                numbers.add(number);

                //Reset the accumulator
                number = 0;
            }
        }

        //Return the list of numbers
        return numbers;
    }

    public static ArrayList<Long> variableByteDecodeLong(byte[] bytes){

        //Array to hold the decoded numbers
        ArrayList<Long> numbers = new ArrayList<>();

        //Accumulator for the current decoded number
        long number = 0;

        //For each byte in the array
        for (byte aByte : bytes) {

            //We use the mask 0x80 = 1000 0000, to check if the MSB of the byte is 1
            if ((aByte & 0x80) == 0x00) {
                //The MSB is 0, then we're not at the end of the sequence of bytes of the code
                number = number * 128 + aByte;
            } else {
                //The MSB is 1, then we're at the end

                //Add to the accumulator number*128 + the integer value in aByte discarding the 1 in the MSB
                number = number * 128 + (aByte &  0x7F);

                //Add the decoded number to the list of numbers
                numbers.add(number);

                //Reset the accumulator
                number = 0;
            }
        }

        //Return the list of numbers
        return numbers;
    }

    public static void main(String[] args) {
        ArrayList<Long> longs = new ArrayList<>();
        longs.add(824L);
        longs.add(5L);
        longs.add(8000000L);
        longs.add(0L);
        longs.add(128L);
        byte[] b = variableByteEncodeLong(longs);
        for (byte value : b) {
            System.out.println(String.format("%8s", Integer.toBinaryString(value & 0xFF)).replace(' ', '0'));
        }

        System.out.println(variableByteDecode(b));
    }
}
