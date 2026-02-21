# TODO: 未対応 SQL 機能

現状サポートしている機能との差分として、一般的な SQL で対応が必要な機能をまとめる。

## DDL（データ定義）

- [ ] DROP TABLE
- [ ] ALTER TABLE (ADD COLUMN, DROP COLUMN, MODIFY COLUMN)
- [ ] PRIMARY KEY 制約
- [ ] UNIQUE 制約
- [ ] DEFAULT 値
- [ ] CHECK 制約
- [ ] FOREIGN KEY 制約
- [ ] CREATE INDEX / DROP INDEX
- [ ] AUTO INCREMENT / SERIAL

## DML（データ操作）

- [x] UPDATE ... SET ... WHERE
- [x] DELETE FROM ... WHERE
- [ ] INSERT でカラムリストを指定 (`INSERT INTO t (col1, col2) VALUES (...)`)
- [ ] INSERT ... SELECT

## SELECT 句

- [ ] DISTINCT
- [x] ORDER BY (ASC / DESC)
- [x] LIMIT / OFFSET
- [ ] GROUP BY
- [ ] HAVING

## JOIN

- [ ] INNER JOIN ... ON
- [ ] LEFT JOIN / RIGHT JOIN
- [ ] CROSS JOIN
- [ ] テーブル別名 (`FROM users u`)

## 集合演算

- [ ] UNION / UNION ALL
- [ ] INTERSECT
- [ ] EXCEPT

## サブクエリ

- [ ] スカラーサブクエリ (`SELECT (SELECT ...)`)
- [ ] IN サブクエリ (`WHERE id IN (SELECT ...)`)
- [ ] EXISTS サブクエリ

## 集約関数

- [ ] SUM
- [ ] AVG
- [ ] MIN
- [ ] MAX

## 演算子・条件式

- [ ] IN (`WHERE id IN (1, 2, 3)`)
- [ ] BETWEEN (`WHERE id BETWEEN 1 AND 10`)
- [ ] LIKE (`WHERE name LIKE '%alice%'`)
- [ ] NOT（一般的な否定。現状は IS NOT NULL のみ）
- [ ] CASE WHEN ... THEN ... ELSE ... END

## データ型

- [ ] BOOLEAN
- [ ] FLOAT / REAL / DOUBLE
- [ ] DECIMAL / NUMERIC
- [ ] DATE / TIME / TIMESTAMP
- [ ] VARCHAR(n)（長さ制限付き文字列）
- [ ] BLOB / BYTEA

## 組み込み関数

- [ ] 文字列関数（LENGTH, UPPER, LOWER, SUBSTRING, TRIM, CONCAT など）
- [ ] 数値関数（ABS, ROUND, MOD など）
- [ ] COALESCE / NULLIF
- [ ] CAST（型変換）

## トランザクション

- [ ] BEGIN / COMMIT / ROLLBACK
- [ ] SAVEPOINT

## その他

- [ ] VIEW（CREATE VIEW / DROP VIEW）
- [ ] ウィンドウ関数（ROW_NUMBER, RANK など）
- [ ] 文字列連結演算子（`||`）
- [ ] EXPLAIN（実行計画の表示）
