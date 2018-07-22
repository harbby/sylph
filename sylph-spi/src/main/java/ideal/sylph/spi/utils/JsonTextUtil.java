package ideal.sylph.spi.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.countMatches;

public final class JsonTextUtil
{
    private JsonTextUtil() {}

    /**
     * 读取json型配置文件
     **/
    public static String readJsonText(File file)
            throws IOException
    {
        String text = parserJson(Files.lines(file.toPath(), UTF_8));
        return text;
    }

    /**
     * 去掉json字符串中的注释
     */
    public static String readJsonText(String json)
    {
        return parserJson(Arrays.stream(json.split("\n")));
    }

    private static String parserJson(Stream<String> stream)
    {
        final StringBuilder text = new StringBuilder();
        stream.map(line -> {
            /*  计算出 发现// 时前面有多少个"  */
            return Arrays.stream(line.split("//")).reduce((x1, x2) -> {
                if (countMatches(x1, "\"") % 2 == 0) {  //计算出 "在 x1中出现的次数
                    return x1;
                }
                else {
                    return x1 + "//" + x2;
                }
            }).orElse("");
        }).filter(x -> !x.trim().equals("")).forEach(x -> text.append(x).append("\n"));
        return text.toString();
    }
}
