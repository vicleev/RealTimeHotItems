package utils;

import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.javatuples.Triplet;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by Administrator on 2019/2/14.
 */
public class CsvFormatter {

    /** 用户行为数据结构 **/
    public static class UserBehavior {
        public long userId;         // 用户ID
        public long itemId;         // 商品ID
        public int categoryId;      // 商品类目ID
        public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
        public long timestamp;      // 行为发生的时间戳，单位秒
    }

    public static Triplet<PojoCsvInputFormat<UserBehavior>,PojoTypeInfo<UserBehavior>,Path> format(){
        // UserBehavior.csv 的本地文件路径, 在 resources 目录下
        URL fileUrl = CsvFormatter.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = null;
        try {
            filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        return new Triplet<PojoCsvInputFormat<UserBehavior>,PojoTypeInfo<UserBehavior>,Path>(csvInput,pojoType,filePath);
    }

}
