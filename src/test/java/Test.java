import java.util.Date;

public class Test {
    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        Date date = new Date(System.currentTimeMillis());
        System.out.println(date);
        System.out.println(new Date(999999999999l));
        System.out.println(new Date(10000000000000l));
        System.out.println(new Date(-999999999999l));
    }
}
