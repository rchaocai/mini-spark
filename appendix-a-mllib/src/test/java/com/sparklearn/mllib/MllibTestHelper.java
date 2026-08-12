package com.sparklearn.mllib;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试辅助工具——加载 Iris 数据集。
 */
public final class MllibTestHelper {

    private MllibTestHelper() {
    }

    static List<LabeledPoint> loadIrisData() {
        List<LabeledPoint> data = new ArrayList<>();
        try (InputStream is = MllibTestHelper.class.getClassLoader().getResourceAsStream("iris_binary.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                if (parts.length != 5) continue;

                double[] features = new double[4];
                for (int i = 0; i < 4; i++) {
                    features[i] = Double.parseDouble(parts[i]);
                }
                double label = Double.parseDouble(parts[4]);
                data.add(new LabeledPoint(features, label));
            }
        } catch (Exception e) {
            throw new RuntimeException("加载 Iris 数据集失败", e);
        }
        return data;
    }
}
