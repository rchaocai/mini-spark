package com.sparklearn.sql.execution;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;

import java.util.Iterator;
import java.util.List;

/**
 * 把一段连续的、都支持 codegen 的物理算子融合进一个生成的 Java 类里执行。
 *
 * <p>它包裹一棵 CodegenSupport 子树，在 doExecute 时：
 * <ol>
 *   <li>新建 CodegenContext，调 child.produce(ctx, null) 得到循环体源码</li>
 *   <li>把循环体包进一个实现了 Iterator<Row> 的类模板</li>
 *   <li>用 Janino 编译这个类</li>
 *   <li>对输入 RDD 做 mapPartitions，每个分区用编译出来的类处理迭代器</li>
 * </ol>
 *
 * <p>这一层不改变查询语义，只把"多个嵌套的 RDD.map/filter 迭代器"换成"一段编译出来的循环"。
 */
public final class WholeStageCodegenExec implements PhysicalPlan {

    private static final String GENERATED_CLASS_NAME = "GeneratedIteratorForStage";
    private static final String GENERATED_PACKAGE = "generated";

    private final CodegenSupport child;
    private String cachedSource;

    public WholeStageCodegenExec(CodegenSupport child) {
        this.child = child;
    }

    public CodegenSupport child() {
        return child;
    }

    /**
     * 返回生成的完整 Java 源码，供调试和教学打印。
     */
    public String generatedSource() {
        if (cachedSource == null) {
            CodegenContext ctx = new CodegenContext();
            String body = child.produce(ctx, null);
            cachedSource = formatSource(body);
        }
        return cachedSource;
    }

    @Override
    public RDD<Row> execute() {
        String source = generatedSource();
        // 编译一次，所有分区共用同一个 Class
        Class<?> generatedClass = CodeGenerator.compile(GENERATED_CLASS_NAME, source);

        RDD<Row> inputRdd = child.inputRDDs().get(0);
        return inputRdd.mapPartitions(inputIter -> {
            try {
                @SuppressWarnings("unchecked")
                Iterator<Row> iter = (Iterator<Row>)
                        generatedClass.getDeclaredConstructor(Iterator.class)
                                .newInstance(inputIter);
                return iter;
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to instantiate generated class " + GENERATED_CLASS_NAME, e);
            }
        });
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public String nodeName() {
        return "WholeStageCodegenExec";
    }

    @Override
    public String detailString() {
        return "";
    }

    private String formatSource(String body) {
        return """
                package %s;

                import com.sparklearn.sql.Row;
                import java.util.ArrayList;
                import java.util.Iterator;
                import java.util.List;
                import java.util.NoSuchElementException;

                /**
                 * 由 WholeStageCodegenExec 自动生成的迭代器。
                 * 把一棵物理算子子树融合进一个 processNext 循环，消除中间迭代器层数。
                 */
                public final class %s implements Iterator<Row> {

                    private final Iterator<Row> input;
                    private final List<Row> output = new ArrayList<>();
                    private int outputIndex = 0;
                    private boolean processed = false;

                    public %s(Iterator<Row> input) {
                        this.input = input;
                    }

                    private void processNext() {
                %s
                    }

                    @Override
                    public boolean hasNext() {
                        if (!processed) {
                            processNext();
                        }
                        return outputIndex < output.size();
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        return (Row) output.get(outputIndex++);
                    }
                }
                """.formatted(
                GENERATED_PACKAGE,
                GENERATED_CLASS_NAME,
                GENERATED_CLASS_NAME,
                indent(body, "        "));
    }

    /**
     * 把多行代码体按方法缩进对齐。
     */
    private String indent(String code, String prefix) {
        StringBuilder result = new StringBuilder();
        String[] lines = code.split("\n");
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                result.append("\n");
            }
            if (lines[i].isEmpty()) {
                result.append(prefix);
            } else {
                result.append(prefix).append(lines[i]);
            }
        }
        return result.toString();
    }
}
