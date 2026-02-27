# TODO: 未対応 SQL 機能

現状サポートしている機能との差分として、一般的な SQL で対応が必要な機能をまとめる。

## DDL（データ定義）

- [x] DROP TABLE
- [x] TRUNCATE TABLE
- [x] ALTER TABLE (ADD COLUMN, DROP COLUMN)
- [x] PRIMARY KEY 制約
- [x] UNIQUE 制約
- [x] DEFAULT 値
- [ ] CHECK 制約
- [ ] FOREIGN KEY 制約
- [x] CREATE INDEX / DROP INDEX（複合インデックス対応）
- [ ] AUTO INCREMENT / SERIAL

## DML（データ操作）

- [x] UPDATE ... SET ... WHERE
- [x] DELETE FROM ... WHERE
- [x] INSERT でカラムリストを指定 (`INSERT INTO t (col1, col2) VALUES (...)`)
- [x] INSERT ... SELECT

## SELECT 句

- [x] DISTINCT
- [x] ORDER BY (ASC / DESC)
- [x] LIMIT / OFFSET
- [x] GROUP BY
- [x] HAVING

## JOIN

- [x] INNER JOIN ... ON
- [x] LEFT JOIN
- [x] RIGHT JOIN
- [x] CROSS JOIN
- [x] テーブル別名 (`FROM users u`)

## 集合演算

- [x] UNION / UNION ALL
- [x] INTERSECT
- [x] EXCEPT

## サブクエリ

- [x] スカラーサブクエリ (`SELECT (SELECT ...)`)
- [x] IN サブクエリ (`WHERE id IN (SELECT ...)`)
- [x] EXISTS サブクエリ
- [x] FROM サブクエリ (`SELECT * FROM (SELECT ...) AS sub`)

## 集約関数

- [x] SUM
- [x] AVG
- [x] MIN
- [x] MAX

## 演算子・条件式

- [x] IN (`WHERE id IN (1, 2, 3)`)
- [x] BETWEEN (`WHERE id BETWEEN 1 AND 10`)
- [x] LIKE (`WHERE name LIKE '%alice%'`)
- [x] NOT（一般的な否定）
- [x] CASE WHEN ... THEN ... ELSE ... END

## データ型

- [ ] BOOLEAN
- [x] FLOAT / REAL / DOUBLE
- [ ] DECIMAL / NUMERIC
- [ ] DATE / TIME / TIMESTAMP
- [ ] VARCHAR(n)（長さ制限付き文字列）
- [ ] BLOB / BYTEA

## 組み込み関数

- [x] 文字列関数（LENGTH, UPPER, LOWER, SUBSTRING, TRIM, CONCAT など）
- [x] 数値関数（ABS, ROUND, MOD, CEIL, FLOOR, POWER）
- [x] COALESCE / NULLIF
- [x] CAST（型変換）

## トランザクション

- [ ] BEGIN / COMMIT / ROLLBACK
- [ ] SAVEPOINT

## データベース管理

- [x] CREATE DATABASE / DROP DATABASE
- [x] USE DATABASE
- [x] SHOW DATABASES
- [x] SHOW TABLES
- [x] クロスデータベース テーブル参照 (`db.table` 構文)

## その他

- [ ] VIEW（CREATE VIEW / DROP VIEW）
- [x] ウィンドウ関数（ROW_NUMBER, RANK, DENSE_RANK, SUM/COUNT/AVG/MIN/MAX OVER）
- [ ] 文字列連結演算子（`||`）
- [ ] EXPLAIN（実行計画の表示）
