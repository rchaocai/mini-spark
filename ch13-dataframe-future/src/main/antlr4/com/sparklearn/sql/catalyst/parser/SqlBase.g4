/*
 * 教学版 SQL 语法（精简自 Spark 的 SqlBase.g4）。
 *
 * 参考 Spark 源码：
 *   sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4
 *
 * 仅覆盖教学所需子集：
 *   SELECT [DISTINCT] col [, col | count(*) | expr] FROM relation
 *   [WHERE expr]
 *   [GROUP BY col [, col]]
 *
 * relation 支持 JOIN：
 *   tableName [ (INNER|LEFT [OUTER]) JOIN tableName ON col = col [AND ...] ]*
 *
 * 列引用支持限定列名（JOIN 同名列歧义时用）：
 *   column | table.column
 *
 * 表达式支持：
 *   - 比较  =, ==, <>, !=, <, <=, >, >=
 *   - 逻辑  AND / OR / NOT
 *   - 算术  + - * /
 *   - 字面量  数字、字符串、布尔、NULL
 *   - 标识符  column
 *   - 函数调用  count(*) / count(expr) / sum(expr) ...
 */

grammar SqlBase;

tokens { DELIMITER }

// 顶层入口：解析一条语句
singleStatement
    : statement EOF
    ;

// 顶层入口：解析一个表达式（供 parseExpression 使用）
singleExpression
    : expression EOF
    ;

statement
    : query
    ;

query
    : querySpecification
    ;

// SELECT ... FROM ... [WHERE ...] [GROUP BY ...]
querySpecification
    : SELECT setQuantifier? namedExpressionSeq
      FROM relation
      (WHERE where=booleanExpression)?
      aggregation?
    ;

// FROM 子句：一个表名，后面可以跟任意多个 JOIN
relation
    : left=identifier (joinRelation)*
    ;

// JOIN 子句：[INNER|LEFT [OUTER]] JOIN tableName ON condition
joinRelation
    : joinType? JOIN right=identifier ON condition=booleanExpression
    ;

joinType
    : INNER
    | LEFT OUTER?
    ;

aggregation
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)*
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

namedExpressionSeq
    : namedExpression (',' namedExpression)*
    ;

// SELECT 项：可以带别名
namedExpression
    : expression (AS? alias=identifier)?
    ;

// 表达式层级（自上而下优先级递增）
expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                            #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression      #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression       #logicalBinary
    | predicated                                                       #booleanDefault
    ;

predicated
    : valueExpression
    ;

valueExpression
    : primaryExpression                                                #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                          #arithmeticUnary
    | left=valueExpression
        operator=(ASTERISK | SLASH | PERCENT) right=valueExpression    #arithmeticBinary
    | left=valueExpression
        operator=(PLUS | MINUS) right=valueExpression                  #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression    #comparison
    ;

primaryExpression
    : constant                                                         #constantDefault
    | ASTERISK                                                         #star
    | functionName=identifier
        '(' (setQuantifier? expression (',' expression)*)? ')'         #functionCall
    | multipartIdentifier                                              #columnReference
    | '(' expression ')'                                               #parenthesizedExpression
    ;

constant
    : NULL                                                             #nullLiteral
    | number                                                           #numericLiteral
    | booleanValue                                                     #booleanLiteral
    | STRING                                                           #stringLiteral
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

// 限定列名：table.column 或单列名（JOIN 同名列歧义时用 table.column 限定）
multipartIdentifier
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER                                                       #unquotedIdentifier
    | quotedIdentifier                                                 #quotedIdentifierAlternative
    | nonReserved                                                      #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : MINUS? DECIMAL_VALUE      #decimalLiteral
    | MINUS? INTEGER_VALUE      #integerLiteral
    ;

// 这里把关键字作为非保留字，允许它们作为列名使用
nonReserved
    : SELECT | FROM | WHERE | GROUP | BY | HAVING | AS | DISTINCT | ALL
    | AND | OR | NOT | TRUE | FALSE | NULL
    | JOIN | ON | INNER | LEFT | OUTER
    ;

// ============================================================================
// Lexer 规则
// ============================================================================

SELECT: 'SELECT';
FROM: 'FROM';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
HAVING: 'HAVING';
AS: 'AS';
DISTINCT: 'DISTINCT';
ALL: 'ALL';
AND: 'AND';
OR: 'OR';
NOT: 'NOT';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULL: 'NULL';
JOIN: 'JOIN';
ON: 'ON';
INNER: 'INNER';
LEFT: 'LEFT';
OUTER: 'OUTER';

EQ  : '=' | '==';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';

STRING
    : '\'' (~('\'' | '\\') | '\\' .)* '\''
    | '"' (~('"' | '\\') | '\\' .)* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

BACKQUOTED_IDENTIFIER
    : '`' (~'`' | '``')* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

UNRECOGNIZED
    : .
    ;
