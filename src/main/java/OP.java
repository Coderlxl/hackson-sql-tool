import java.util.ArrayList;

/**
 * 封装 SQL 执行结果
 */
public class OP {

    public static String opStatus(boolean success, String errorMsg) {
        return "{\"status\":" + success + ",\"errorMsg\":\"" + errorMsg + "\"}";
    }

    public static String opResult(ArrayList<String[]> list, String errorMsg) {
        StringBuilder resultBuilder = new StringBuilder();

        for (int j = 0; j < list.size(); j++) {
            String[] line = list.get(j);

            StringBuilder builder = new StringBuilder();
            builder.append("[");
            for (int i = 0; i < line.length; i++) {
                builder.append("\"" + line[i] + "\"");

                if (i != line.length - 1) {
                    builder.append(",");
                }
            }
            builder.append("]");

            resultBuilder.append(builder.toString());
            if (j != list.size() - 1) {
                resultBuilder.append(",");
            }
        }

        String result = resultBuilder.toString();
        return "{\"result\":[" + result + "],\"errorMsg\":\"" + errorMsg + "\"}";
    }
}
