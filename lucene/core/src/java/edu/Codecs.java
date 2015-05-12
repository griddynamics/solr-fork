package edu;

public class Codecs {

    static {
        try {
          System.load("/home/perf/projects/SIMD4Lucene/native/linux/target/libutil.so");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load from path '" + System.getProperty("java.library.path") + "'" + e);
            System.exit(1);
        }
    }

    /**
     * Compresses data from datain to buffer, returns how many bytes written.
     */
    public native static int compressRegion(int[] datain, byte[] buffer);

    public native static int compressElements(int[] datain, byte[] buffer);

    public native static int compressElementsCritical(int[] datain, byte[] buffer);

    /**
     * Encodes sorted lists of integers in {@code pfor} format.
     *
     * @param data   the data to encode
     * @param length the number of bytes to encode
     * @param buffer a buffer to use to encode data
     * @return how many bytes are written.
     */
    public native static int encodeCritical(int[] data, int offset, int length, byte[] buffer);

    /**
     * Decodes sorted lists of integers from {@code buffer}.
     *
     * @param data   a buffer to use to decode data
     * @param length the number of bytes to encode
     * @param result   the data to encode
     * @return how many bytes are read.
     */
    public native static int decodeCritical(byte[] data, int length, int[] result);

    public native static int compressCritical(int[] data, byte[] encoded);

    public native static int uncompressRegion(int[] dataout, byte[] buffer);

    public native static int uncompressElements(int[] dataout, byte[] buffer);

    public native static int uncompressElementsCritical(int[] dataout, byte[] buffer);

    public native static int uncompressCritical(int[] dataout, byte[] buffer);
}
