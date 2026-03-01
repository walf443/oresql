# TODO: 未対応 SQL 機能

現状サポートしている機能との差分として、一般的な SQL で対応が必要な機能をまとめる。

## DDL（データ定義）

- [x] DROP TABLE
- [x] TRUNCATE TABLE
- [x] ALTER TABLE (ADD COLUMN, DROP COLUMN)
- [ ] ALTER TABLE RENAME TABLE / RENAME COLUMN
- [x] PRIMARY KEY 制約
- [x] NOT NULL 制約
- [x] UNIQUE 制約
- [x] DEFAULT 値
- [ ] CHECK 制約
- [ ] FOREIGN KEY 制約
- [x] CREATE INDEX / DROP INDEX（複合インデックス対応）
- [ ] AUTO INCREMENT / SERIAL
- [ ] IF EXISTS / IF NOT EXISTS（CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS）

## DML（データ操作）

- [x] UPDATE ... SET ... WHERE
- [x] DELETE FROM ... WHERE
- [x] INSERT でカラムリストを指定 (`INSERT INTO t (col1, col2) VALUES (...)`)
- [x] INSERT ... SELECT
- [ ] INSERT ... ON CONFLICT / ON DUPLICATE KEY UPDATE（UPSERT）
- [ ] REPLACE INTO

## SELECT 句

- [x] DISTINCT
- [ ] DISTINCT ON (col, ...)
- [x] ORDER BY (ASC / DESC)
- [ ] ORDER BY ... NULLS FIRST / NULLS LAST
- [x] LIMIT / OFFSET
- [x] GROUP BY
- [x] HAVING

## JOIN

- [x] INNER JOIN ... ON
- [x] LEFT JOIN
- [x] RIGHT JOIN
- [x] CROSS JOIN
- [x] JOIN ... USING (col1, col2)
- [x] テーブル別名 (`FROM users u`)
- [ ] FULL OUTER JOIN
- [ ] NATURAL JOIN
- [ ] LATERAL JOIN

## 集合演算

- [x] UNION / UNION ALL
- [x] INTERSECT
- [x] EXCEPT

## サブクエリ

- [x] スカラーサブクエリ (`SELECT (SELECT ...)`)
- [x] IN サブクエリ (`WHERE id IN (SELECT ...)`)
- [x] EXISTS サブクエリ
- [x] FROM サブクエリ (`SELECT * FROM (SELECT ...) AS sub`)
- [x] 相関サブクエリ
- [ ] 複数カラム IN (`WHERE (a, b) IN ((1,2), (3,4))`)
- [ ] ANY / SOME / ALL 比較演算子 (`WHERE x > ANY (SELECT ...)`)

## CTE（共通テーブル式）

- [x] WITH ... AS（非再帰 CTE）
- [x] WITH RECURSIVE（再帰 CTE、UNION / UNION ALL 対応）
- [ ] CTE を UPDATE / DELETE で使用

## 集約関数

- [x] COUNT
- [x] SUM
- [x] AVG
- [x] MIN
- [x] MAX
- [ ] GROUP_CONCAT / STRING_AGG

## 演算子・条件式

- [x] IN (`WHERE id IN (1, 2, 3)`)
- [x] BETWEEN (`WHERE id BETWEEN 1 AND 10`)
- [x] LIKE (`WHERE name LIKE '%alice%'`)
- [x] NOT（一般的な否定）
- [x] CASE WHEN ... THEN ... ELSE ... END
- [ ] 文字列連結演算子（`||`）
- [ ] 剰余演算子（`%`）※ MOD 関数は対応済み
- [ ] ILIKE（大文字小文字を区別しない LIKE）
- [ ] SIMILAR TO / 正規表現マッチ

## データ型

- [x] BOOLEAN
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
- [ ] 日付・時刻関数（NOW, DATE, EXTRACT など）
- [ ] IFNULL / IIF

## トランザクション

- [ ] BEGIN / COMMIT / ROLLBACK
- [ ] SAVEPOINT

## データベース管理

- [x] CREATE DATABASE / DROP DATABASE
- [x] USE DATABASE
- [x] SHOW DATABASES
- [x] SHOW TABLES
- [x] クロスデータベース テーブル参照 (`db.table` 構文)
- [ ] DESCRIBE / SHOW COLUMNS（テーブル構造の表示）

## その他

- [ ] VIEW（CREATE VIEW / DROP VIEW）
- [x] ウィンドウ関数（ROW_NUMBER, RANK, DENSE_RANK, SUM/COUNT/AVG/MIN/MAX OVER）
- [ ] EXPLAIN（実行計画の表示）
