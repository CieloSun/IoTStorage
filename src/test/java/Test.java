import com.alibaba.fastjson.JSON;
import com.cielo.demo.model.user.User;

import java.util.HashMap;
import java.util.Map;

public class Test {

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
//        List<String> list = new ArrayList<>();
//        list.add(JSON.toJSONString(new User("Cielo", "0928", 0)));
//        list.add(JSON.toJSONString(new User("Boris", "0928", 0)));
//        String s = JSONUtil.toListJSON(list);
//        List<User> users = JSON.parseArray(s, User.class);
//        System.out.println(users.get(0));
//        System.out.println(users.get(1));
//        list.del();
//        list.add(s);
//        list.add(s);
//        s=JSONUtil.mergeList(list);
//        System.out.println(s);
//        users = JSON.parseArray(s, User.class);
//        System.out.println(users.get(0));
//        System.out.println(users.get(1));
//        System.out.println(users.get(2));
//        System.out.println(users.get(3));
        Map<String, String> map = new HashMap<>();
        map.put("Cielo", JSON.toJSONString(new User("Cielo", "0928", 0)));
        map.put("Boris", JSON.toJSONString(new User("Boris", "0928", 0)));
        map.put("Judy", JSON.toJSONString(new User("Judy", "0928", 0)));
        String s1 = map.toString();
        System.out.println(s1);
        System.out.println(JSON.toJSONString(map));
        String s = JSON.toJSONString(map).replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}");
        System.out.println(s);
        System.out.println(JSON.parseObject(s));
        StringBuilder stringBuilder = new StringBuilder("{");
        map.keySet().parallelStream().map(key -> "\"" + key + "\"" + ":" + map.get(key) + ",").forEach(stringBuilder::append);
        stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), "}");
        String s2 = stringBuilder.toString();
        System.out.println(s2);
        System.out.println(JSON.parseObject(s2));
    }
}
