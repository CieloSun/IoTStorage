import com.github.luben.zstd.Zstd;

public class Test {

    private static byte[] compress(byte[] bytes) {
        return Zstd.compress(bytes);
    }

    private static byte[] decompress(byte[] bytes) {
        return Zstd.decompress(bytes, (int) Zstd.decompressedSize(bytes));
    }

    public static void main(String[] args) throws Exception {
        String test = "group1/M00/00/00/3smQqlx47qqASRi2AAACGF9_sNY88329.ngroup1/M00/00/00/3smQqlx47qqASRi2AAACGF9_sNY88329.ngroup1/M00/00/00/3smQqlx47qqASRi2AAACGF9_sNY88329.n";
        byte[] bytes = test.getBytes();
        byte[] compress = compress(bytes);
        System.out.println(compress.length);
        byte[] decompress = decompress(compress);
        System.out.println(decompress.length);
        System.out.println(new String(decompress));
    }
}
