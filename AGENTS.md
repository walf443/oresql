# AGENTS.md

## プロジェクト概要

oresql は Go で書かれたインメモリ SQL データベースエンジン。REPL インターフェースで SQL 文を対話的に実行できる。SQL のパースと実行をスクラッチで実装している。

## アーキテクチャ

SQL 実行パイプライン:

```
入力 -> Lexer -> Parser -> AST -> Executor -> Result -> REPL Writer
```

### パッケージ構成

- **`token/`** — トークン型とキーワード判定。`TokenType`, `Token`, `LookupIdent` を定義。
- **`lexer/`** — 字句解析。SQL テキストをトークン列に変換。
- **`ast/`** — AST ノード定義。`Statement` / `Expr` インターフェースと、`CreateTableStmt`, `InsertStmt`, `SelectStmt`, `BinaryExpr` 等の具象型。
- **`parser/`** — SQL パーサ。Lexer からトークンを読み、AST を生成。
- **`engine/`** — クエリ実行エンジン:
  - `Catalog` — スキーマ情報（テーブル・カラム定義）
  - `Storage` — インメモリの行ストレージ
  - `Executor` — 文の実行、式評価、WHERE フィルタリング
  - `Result` — クエリ結果型（`Columns`, `Rows`, `Message`）
- **`repl/`** — REPL 出力フォーマット。`Writer` が `io.Writer` をラップし、`PrintResult`, `PrintError`, `Println` を提供。
- **`main.go`** — エントリポイント。readline セットアップと REPL ループ。

## 対応 SQL

- `CREATE TABLE <name> (<col> INT|TEXT, ...)`
- `INSERT INTO <name> VALUES (<val>, ...)`
- `SELECT <cols|*> FROM <name> [WHERE <condition>]`
- WHERE は比較演算子（`=`, `!=`, `<`, `>`, `<=`, `>=`）と論理演算子（`AND`, `OR`）をサポート

## データ型

`INT`（Go `int64`）と `TEXT`（Go `string`）の 2 種類。

## 開発フロー

まずできるだけ追加しようとしている要件のテストを追加して現状の仕組みで失敗するのを確認する

確認後実装をして、再度テストが通ることを確認する
テストが通ったのを確認したら全体のテストを実行してレグレッションがないことを確認する

ひととおり開発できたらgo fmtをかけてソースコードを整形する

## ビルド・テスト

```sh
go build ./...
go test ./...
go fmt ./...
```

## 規約

- Go 標準のプロジェクトレイアウト（1 ディレクトリ 1 パッケージ）
- テーブル名・カラム名は大文字小文字を区別しない（内部で小文字化）
- テストは標準 `testing` パッケージでテーブル駆動テスト。`*_test.go` はテスト対象と同じディレクトリに配置
- 外部依存は `github.com/chzyer/readline`（REPL の行編集）のみ
