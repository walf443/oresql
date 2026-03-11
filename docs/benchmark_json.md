# JSON vs JSONB ベンチマークレポート

実行日: 2026-03-11
環境: Apple M2 Pro, darwin/arm64, Go

## 概要

JSON 型（テキスト格納）と JSONB 型（カスタムバイナリ格納）の空間効率・エンコード/デコード性能・部分アクセス性能を比較した。
JSONB はデシリアライズなしで O(log n) の部分アクセスが可能なバイナリフォーマットであり、JSON テキストに対してほぼ同等の空間効率を維持しつつ、クエリ時の部分アクセス性能で大きな優位性を持つ。

### JSONB バイナリフォーマットの主な最適化

- **グローバルキー辞書**: 全オブジェクトキーをヘッダに1回だけ格納し、オブジェクト内は辞書インデックスで参照
- **型付き配列 (Typed Array)**: 整数/浮動小数点のみの配列は要素ごとのタグを省略し、最小幅で格納
- **コンパクト整数エンコーディング**: 値の大きさに応じて 1/2/4/8 バイト幅を選択
- **コンパクト文字列長**: 128 バイト未満は 1 バイト長、以上は 4 バイト長
- **適応幅エントリテーブル**: オブジェクトのキーインデックス・値オフセットを最小幅で格納

## ベンチマーク結果

### 1. 空間効率 (JSON テキスト vs JSONB バイナリ)

| データ | JSON (bytes) | JSONB (bytes) | JSONB/JSON | 備考 |
|--------|-------------|--------------|------------|------|
| SmallObject | 39 | 44 | **112.8%** | `{"name":"alice","age":30,"active":true}` |
| MediumObject | 147 | 168 | **114.3%** | 7 フィールド、ネストあり |
| LargeObject | 754 | 762 | **101.1%** | 配列・ネストオブジェクト含む複雑な構造 |
| ArrayOf100Ints | 291 | 108 | **37.1%** | 整数 100 要素の配列 |
| DeeplyNested | 132 | 179 | **135.6%** | 10 段ネストのオブジェクト |

**考察:** 小さなオブジェクトでは辞書ヘッダやエントリテーブルのオーバーヘッドにより 113-136% になるが、構造が複雑になるほどキー辞書の重複排除が効き、LargeObject では **101.1%** とほぼ同サイズに収束する。整数配列は Typed Array (最小幅パッキング) により **37.1%** と大幅に削減される。

---

### 2. エンコード性能 (JSON → JSONB)

| データ | ns/op | B/op | allocs |
|--------|-------|------|--------|
| SmallObject | 1,314 | 1,608 | 25 |
| MediumObject | 4,125 | 4,224 | 80 |
| LargeObject | 19,405 | 19,432 | 376 |

**考察:** エンコードは JSON パース → キー収集 → 辞書構築 → バイナリ化の多段処理のため、単純な JSON テキスト格納より高コスト。ただしこれは INSERT 時の1回のみのコストであり、読み取り時の部分アクセス高速化で償却される。

---

### 3. デコード性能 (JSONB → Go 値) vs JSON Unmarshal

| データ | JSONB Decode (ns/op) | JSONB (allocs) | JSON Unmarshal (ns/op) | JSON (allocs) | 倍率 |
|--------|---------------------|----------------|----------------------|---------------|------|
| SmallObject | 198 | 8 | 568 | 12 | **2.9x faster** |
| MediumObject | 684 | 31 | 1,598 | 39 | **2.3x faster** |
| LargeObject | 2,969 | 120 | 8,591 | 188 | **2.9x faster** |

**考察:** JSONB のフルデコードは JSON テキストの `json.Unmarshal` と比較して **2.3-2.9 倍**高速。バイナリフォーマットなのでテキストパースのオーバーヘッド（数値変換、文字列エスケープ処理、区切り文字スキップ等）が不要。辞書ヘッダの直接文字列デコード、配列/オブジェクトの中間バッファ排除により、アロケーション数も 33-36% 削減されている。

---

### 4. JSONB → JSON 文字列変換 (ToJSON)

| データ | ns/op | B/op | allocs |
|--------|-------|------|--------|
| SmallObject | 754 | 1,077 | 19 |
| MediumObject | 2,344 | 2,833 | 60 |
| LargeObject | 11,682 | 14,915 | 245 |

**考察:** `ToJSON` は Decode + JSON 文字列化の2段階処理。SELECT 時の出力変換で使用される。

---

### 5. 部分アクセス性能 — LookupKey vs フルデコード

LargeObject に対して単一キーでアクセスする場合:

| 方法 | ns/op | allocs | vs FullDecode |
|------|-------|--------|---------------|
| **LookupKey** | 249 | 16 | **11.4x faster** |
| FullDecode | 2,837 | 120 | baseline |

**考察:** `LookupKey` は辞書の二分探索 → エントリテーブルの二分探索で目的の値だけをデコードする。フルデコード (全フィールドを Go の `map[string]any` に展開) と比較して **11.4 倍**高速。辞書ヘッダの直接文字列デコード最適化により、以前の 6.8x から大幅に改善。

---

### 6. 部分アクセス性能 — LookupIndex (Typed Array)

100 要素の整数配列から 50 番目の要素を取得:

| 方法 | ns/op | allocs | vs FullDecode |
|------|-------|--------|---------------|
| **LookupIndex** | 7.0 | 0 | **60x faster** |
| FullDecode | 420 | 2 | baseline |

**考察:** Typed IntArray は固定幅エンコーディングなので、インデックスから直接バイトオフセットを計算して O(1) でアクセスできる。アロケーションもゼロ。

---

### 7. マルチレベルパス探索 — LookupKeys

LargeObject に対して `$.items[0].product` (3 階層) をアクセスする場合:

| 方法 | ns/op | allocs | vs FullDecode |
|------|-------|--------|---------------|
| **LookupKeys** (2 階層) | 278 | 17 | **10.4x faster** |
| **LookupKeys** (3 階層) | 299 | 17 | **9.7x faster** |
| ChainedLookup (3 階層) | 1,853 | 62 | 1.6x faster |
| FullDecode (3 階層) | 2,894 | 120 | baseline |

**考察:** `LookupKeys` は辞書ヘッダを1回だけパースし、中間値のデコードなしでバイト位置だけを辿る。個別に `LookupKey` をチェーンする方法 (辞書を毎回再パース＋中間値をフルデコード) と比較しても **6.2 倍**高速。深さが増えても辞書パースは1回なのでオーバーヘッドは小さい。

---

### 8. JSON Path 探索 — QueryPath / ExistsPath

`json_path.Path` を使った探索の性能比較:

| 方法 | ns/op | allocs | vs FullDecode+Execute |
|------|-------|--------|----------------------|
| **QueryPath** (2 階層) | 319 | 19 | **8.8x faster** |
| **ExistsPath** (2 階層) | 102 | 2 | **27.7x faster** |
| **QueryPath** (3 階層) | 336 | 20 | **8.4x faster** |
| **ExistsPath** (3 階層) | 107 | 2 | **26.4x faster** |
| json_path.Execute (デコード済み) | 20.6 | 0 | — |
| json_path.Exists (デコード済み) | 20.5 | 0 | — |
| FullDecode + Execute (3 階層) | 2,821 | 120 | baseline |

**考察:** `QueryPath`/`ExistsPath` は JSONB バイナリから直接パスを辿る。`ExistsPath` は辞書のバイトオフセットテーブルを構築して文字列デコードなしでキーを照合し、最終値のデコードも省略するため **26-28 倍**高速（2 allocs のみ）。`QueryPath` は最終値のデコードに辞書が必要なため `readDictHeader` を使うが、それでも **8.4-8.8 倍**高速。

`json_path.Execute` 自体は 20.6 ns と非常に高速だが、これはデコード済みの `map[string]any` に対する操作であり、デコードコスト (2,821 ns) が支配的。JSONB の部分アクセスはこのデコードコストを回避できる点が最大の利点。

---

### 9. SQL クエリ実行性能 — JSON 型 vs JSONB 型 (100 行テーブル)

実際の SQL クエリ実行における JSON 型と JSONB 型の性能差を計測した。テーブルは 100 行、各行に 5 フィールド (name, age, tags, address) のオブジェクトを格納。

| クエリ | JSON (ns/op) | JSON (allocs) | JSONB (ns/op) | JSONB (allocs) | 倍率 |
|--------|-------------|---------------|--------------|----------------|------|
| `JSON_VALUE(data, '$.name')` | 144,548 | 3,820 | 32,162 | 1,520 | **4.5x faster** |
| `JSON_VALUE(data, '$.address.city')` | 151,827 | 3,920 | 37,204 | 1,720 | **4.1x faster** |
| `JSON_QUERY(data, '$.tags')` | 164,181 | 4,020 | 60,603 | 2,320 | **2.7x faster** |
| `JSON_EXISTS(data, '$.name')` | 146,693 | 3,720 | 18,222 | 520 | **8.1x faster** |
| `WHERE JSON_EXISTS(data, '$.address.city')` | 152,806 | 3,824 | 26,793 | 724 | **5.7x faster** |

**考察:** JSONB カラムでは `jsonb.QueryPath` / `jsonb.ExistsPath` によりバイナリから直接パスを辿る。JSON カラムでは毎行 `json.Unmarshal` → Go 値 → パス探索が必要なのに対し、JSONB では辞書の二分探索とエントリテーブルの二分探索だけで目的の値に到達する。

`JSON_EXISTS` が最も効果が大きい (**8.1x**) のは、`ExistsPath` が辞書のバイトオフセットテーブルによる文字列デコード不要のキー照合と最終値のデコード省略により、アロケーション 2 回のみで完了するため。`JSON_QUERY` の改善幅が比較的小さい (**2.7x**) のは、結果のオブジェクト/配列を `json.Marshal` で文字列化する必要があるため。

アロケーション数も 56-86% 削減されており、GC 負荷の軽減にも寄与する。

---

### 10. JSON が有利なワークロード — INSERT・SELECT (パスアクセスなし)

JSON 型が有利になるケースとして、パスアクセスを伴わないワークロードを計測した。

| クエリ | JSON (ns/op) | JSON (allocs) | JSONB (ns/op) | JSONB (allocs) | 倍率 |
|--------|-------------|---------------|--------------|----------------|------|
| `INSERT INTO docs VALUES (...)` | 1,388 | 22 | 4,331 | 84 | JSON が **3.1x faster** |
| `SELECT data FROM docs` (100行) | 5,520 | 117 | 188,668 | 4,917 | JSON が **34.2x faster** |
| `SELECT data FROM docs WHERE id = 50` | 442 | 12 | 2,310 | 60 | JSON が **5.2x faster** |
| `SELECT id FROM docs` (JSON列不使用) | 7,380 | 221 | 7,344 | 221 | **同等** |

**考察:**

- **INSERT**: JSONB は JSON テキスト → バイナリ変換 (パース + 辞書構築 + エンコード) が必要なため **3.1 倍**遅い。書き込み頻度が高いワークロードでは JSON 型の方が有利。
- **SELECT data (全行)**: JSONB は出力時に `ToJSON` (バイナリ → JSON テキスト変換) が 100 行分発生するため **34.2 倍**の差。JSON カラムをそのまま返すだけのワークロードでは JSON 型が圧倒的に有利。
- **SELECT data WHERE id = 50 (PK lookup)**: 1 行のみだが、JSONB→JSON 変換のコストにより **5.2 倍**遅い。
- **SELECT id (JSON列不使用)**: JSON カラムに一切触れないクエリでは、型に関わらず性能は**同等**。アロケーション数も完全に一致しており、格納型の違いがクエリ性能に影響しないことを確認。

---

## まとめ

| 観点 | JSON | JSONB |
|------|------|-------|
| 格納サイズ (一般的なオブジェクト) | baseline | **101-136%** (ほぼ同等) |
| 格納サイズ (整数配列) | baseline | **37%** (大幅削減) |
| INSERT コスト | テキストそのまま | バイナリ変換あり (高コスト) |
| フルデコード | `json.Unmarshal` | **2.3-2.9x faster** |
| 単一キーアクセス (jsonb pkg) | フルデコード必須 | **11.4x faster** |
| 配列インデックスアクセス (jsonb pkg) | フルデコード必須 | **60x faster** |
| パス探索 (jsonb pkg, 3 階層) | フルデコード必須 | **8.4-8.8x faster** |
| 存在チェック (jsonb pkg, 3 階層) | フルデコード必須 | **26-28x faster** (辞書デコード不要) |
| SQL JSON_VALUE (100 行) | baseline | **4.1-4.5x faster** |
| SQL JSON_QUERY (100 行) | baseline | **2.7x faster** |
| SQL JSON_EXISTS (100 行) | baseline | **5.7-8.1x faster** |
| SQL INSERT | **3.1x faster** | バイナリ変換コスト |
| SQL SELECT data 全行 (パスなし) | **34.2x faster** | ToJSON 変換コスト |
| SQL SELECT data PK lookup (パスなし) | **5.2x faster** | ToJSON 変換コスト |
| SQL SELECT id (JSON列不使用) | 同等 | 同等 |

JSONB は INSERT 時のエンコードコストと引き換えに、読み取り時の部分アクセスで大きな性能優位性を持つ。フルデコードでも JSON テキストの Unmarshal より 2.3-2.9 倍高速であり、jsonb パッケージレベルの部分アクセスでは 8-60 倍の高速化を実現する。特に `ExistsPath` は辞書のバイトオフセット参照により文字列デコードを完全に回避し、26-28 倍の高速化を達成。実際の SQL クエリ実行でも JSON_VALUE で 4.5 倍、JSON_EXISTS で 8.1 倍の高速化が確認されており、JSON_QUERY、JSON_VALUE、JSON_EXISTS などの関数でパス指定のアクセスが頻繁に行われるワークロードにおいて、JSONB 型の利点が顕著に現れる。

一方、パスアクセスを伴わないワークロードでは JSON 型が有利。INSERT は 3.1 倍、SELECT data (全行返却) は 34.2 倍 JSON 型の方が高速。これは JSONB のエンコード/デコード変換コストが支配的になるため。JSON 列に触れないクエリ (`SELECT id`) では両型の性能は同等。

**使い分けの指針:** `JSON_VALUE`/`JSON_QUERY`/`JSON_EXISTS` によるパスアクセスが頻繁なら JSONB、INSERT が多く JSON カラムをそのまま返すワークロードなら JSON が適している。
