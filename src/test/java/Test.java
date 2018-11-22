import com.alibaba.fastjson.JSON;
import com.cielo.model.User;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

public class Test {

    public static String mergeJson(List<String> jsonObjects){
        StringBuilder stringBuilder = new StringBuilder("[");
        IntStream.range(0, jsonObjects.size()).forEach(i -> {
            stringBuilder.append(jsonObjects.get(i));
            if (i < jsonObjects.size() - 1) stringBuilder.append(",");
        });
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
    public static void main(String[] args) {
//        System.out.println(System.currentTimeMillis());
//        Date date = new Date(System.currentTimeMillis());
//        System.out.println(date);
//        System.out.println(new Date(999999999999l));
//        System.out.println(new Date(10000000000000l));
//        System.out.println(new Date(-999999999999l));
//        System.out.println("测试中文".getBytes());
//        System.out.println(new String("测试中文".getBytes()));
//        String fdfs = "group1/M00/00/00/wKgRcFV_08OAK_KCAAAA5fm_sy874";
//        String[] strings = fdfs.split("/", 2);
//        System.out.println(strings[0]);
//        System.out.println(strings[1]);
//        System.out.println(JSON.toJSONString(strings));
        List<String> list=new ArrayList<>();
        list.add(JSON.toJSONString(new User("Cielo", "0928", 0)));
        list.add(JSON.toJSONString(new User("Boris", "0928", 0)));
        String s = mergeJson(list);
        List<User> users = JSON.parseArray(s, User.class);
        System.out.println(users.get(0));
        System.out.println(users.get(1));

    }
}
