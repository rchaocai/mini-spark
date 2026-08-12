/*
 * 附录 B 演示用语法：算术表达式。
 *
 * 支持：整数、加减乘除、括号。
 * 例：1 + 2 * 3      → 7
 *     (1 + 2) * 3    → 9
 *     10 / 2 - 1     → 4
 */
grammar Expr;

// 入口：一个表达式后跟 EOF
prog
    : expr EOF
    ;

// 用 # 标签给每个备选分支命名，ANTLR 会为每个标签生成对应的 visit 方法：
//   visitMulDiv、visitAddSub、visitInt、visitParens
// op 是对操作符 token 的命名，visit 时用 ctx.op.getType() 判断是哪个操作符
expr
    : expr op=(MUL | DIV) expr   # MulDiv
    | expr op=(ADD | SUB) expr   # AddSub
    | INT                          # Int
    | '(' expr ')'                 # Parens
    ;

// ---- 词法规则 ----

MUL : '*' ;
DIV : '/' ;
ADD : '+' ;
SUB : '-' ;
INT : [0-9]+ ;
WS  : [ \t\r\n]+ -> skip ;
