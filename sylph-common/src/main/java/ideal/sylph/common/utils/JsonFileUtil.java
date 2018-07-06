package ideal.sylph.common.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.countMatches;

public final class JsonFileUtil
{
    private JsonFileUtil() {}

    /**
     * 读取json型配置文件
     **/
    public static String readJsonFile(String filePath)
            throws IOException
    {
        final File file = new File(filePath);
        return readJsonFile(file);
    }

    public static String readJsonFile(File file)
            throws IOException
    {
        final StringBuilder text = new StringBuilder();

        FileUtils.readLines(file, UTF_8).stream().map(line -> {
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
        System.out.println(text);
        return text.toString();
    }

    /**
     * 去掉注释
     */
    public static String parserJSON(String json)
    {
        final StringBuilder text = new StringBuilder();

        Arrays.stream(json.split("\n")).map(line -> {
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
