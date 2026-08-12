package com.sparklearn.tools;

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * 遍历 parse tree，计算算术表达式的值。
 *
 * <p>每个 visitXxx 方法对应 Expr.g4 里一个 # 标签的备选分支。
 * 返回类型是 Integer——每棵子树都计算出一个整数值。
 */
class EvalVisitor extends ExprBaseVisitor<Integer> {

    @Override
    public Integer visitProg(ExprParser.ProgContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public Integer visitMulDiv(ExprParser.MulDivContext ctx) {
        int left = visit(ctx.expr(0));
        int right = visit(ctx.expr(1));
        if (ctx.op.getType() == ExprLexer.MUL) {
            return left * right;
        }
        return left / right;
    }

    @Override
    public Integer visitAddSub(ExprParser.AddSubContext ctx) {
        int left = visit(ctx.expr(0));
        int right = visit(ctx.expr(1));
        if (ctx.op.getType() == ExprLexer.ADD) {
            return left + right;
        }
        return left - right;
    }

    @Override
    public Integer visitInt(ExprParser.IntContext ctx) {
        return Integer.parseInt(ctx.INT().getText());
    }

    @Override
    public Integer visitParens(ExprParser.ParensContext ctx) {
        return visit(ctx.expr());
    }
}
