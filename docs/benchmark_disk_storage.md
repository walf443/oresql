# ディスクストレージ ベンチマークレポート

実行日: 2026-03-02
環境: Apple M2 Pro, darwin/arm64, Go

## 概要

disk ストレージ (`--storage disk`) と memory ストレージ (デフォルト) の性能差を、クエリパターン別に計測した。
disk ストレージはデータをページ単位でディスクに格納し、バッファプール (LRU 256 ページ) でホットページのみメモリに保持する。
memory ストレージのセカンダリインデックスはメモリ上の B-tree、disk ストレージのセカンダリインデックスは同一 `.db` ファイル内にページベースの B+Tree として永続化される（DiskSecondaryBTree）。disk ではインデックスの検索自体もバッファプール経由のページフェッチとなるが、Primary / Secondary 両 B+Tree のインラインページスキャン最適化（ページバッファ上で直接オフセット計算して binary search / エントリ走査し、Go ヒープへのアロケーションを排除）と `DecodeRowN` プリアロケーション最適化（カラム数を事前に渡して Row スライスの再割り当てを排除）により、ポイントルックアップのオーバーヘッドは Memory 比 1.3〜1.7x に抑えられている。

## ベンチマーク結果

### 1. PK / セカンダリインデックス ポイントルックアップ (10,000行)

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory | Memory (B/op) | Disk (B/op) |
|---------|---------------|-------------|-------------|--------------|------------|
| PK lookup (`WHERE id = X`) | 828 | 1,113 | **1.3x** | 920 | 1,208 |
| セカンダリインデックス (`WHERE val = X`) | 1,044 | 1,818 | **1.7x** | 1,049 | 1,482 |

**考察:** PK ルックアップは disk で **1.3x**（以前は 2.3x）。Primary B+Tree の `findLeaf` でページバッファ上の直接オフセット計算による inline binary search を行い、`decodeInternalPage` の `make([]int64, n)` + `make([]uint32, n+1)` アロケーションを完全に排除した。B/op が 4,280 → 1,208（**3.5x 削減**）、allocs/op が 32 → 30。セカンダリインデックスのルックアップも **1.7x**（以前は 2.4x）に改善。DiskSecondaryBTree のインラインページスキャン最適化に加え、最終的な PK ルックアップ（`findLeaf` 経由）のオーバーヘッドも削減されたことで、B/op が 4,554 → 1,482（**3.1x 削減**）。

---

### 2. 等値検索 (WHERE val = X)

| データ量 | 条件 | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|------|---------------|-------------|-------------|
| 1,000行 | インデックスなし | 46,192 | 121,311 | **2.6x** |
| 1,000行 | インデックスあり | 1,090 | 1,647 | **1.5x** |
| 10,000行 | インデックスなし | 605,703 | 1,514,844 | **2.5x** |
| 10,000行 | インデックスあり | 1,052 | 1,824 | **1.7x** |

**考察:** インデックスありの場合、Primary / Secondary 両 B+Tree のインラインページスキャン最適化 + `DecodeRowN` プリアロケーションにより 1,000 行では **1.5x**、10,000 行では **1.7x**（以前は 1.6x / 2.4x）。PK ルックアップ (1.3x) に近い水準まで改善されており、セカンダリインデックスの永続化によるオーバーヘッドは実用上ほぼ気にならないレベルに到達した。

---

### 3. 範囲検索 (WHERE val >= X AND val <= Y, ヒット率 20%)

| データ量 | 条件 | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|------|---------------|-------------|-------------|
| 10,000行 | インデックスなし | 971,165 | 1,737,586 | **1.8x** |
| 10,000行 | インデックスあり | 401,398 | 502,223 | **1.3x** |

**考察:** インデックスなしのフルスキャンは 1.8x。インデックスありは `GetByKeysSorted` のバッチフェッチ最適化（ソート済みキーでリーフチェーン1パス走査）+ `DecodeRowN` プリアロケーションにより **1.3x** に改善。多数行ヒット (2,000行) ではプライマリ B+Tree のリーフ走査コストが支配的であり、セカンダリインデックスの検索コスト増は相対的に小さい。

---

### 4. IN 検索

#### 4a. 低カーディナリティ (WHERE category IN (5, 10, 15)) — 300行ヒット

| データ量 | 条件 | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|------|---------------|-------------|-------------|
| 10,000行 | インデックスなし | 816,699 | 1,659,789 | **2.0x** |
| 10,000行 | インデックスあり | 49,447 | 161,609 | **3.3x** |

#### 4b. ユニークカラム (WHERE val IN (50, 100, 150)) — 3行ヒット

| データ量 | 条件 | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|------|---------------|-------------|-------------|
| 10,000行 | インデックスなし | 653,649 | 1,477,755 | **2.3x** |
| 10,000行 | インデックスあり | 1,777 | 2,719 | **1.5x** |

**考察:** ヒット件数が少ない場合（3行）は、Primary / Secondary 両 B+Tree のインラインページスキャン最適化により **1.5x** に改善（以前は 1.9x → 9.2x から段階的に改善）。各 IN 値ごとの PK ルックアップ + DiskSecondaryBTree の PrefixScan の両方でアロケーションが排除されたことが大きい（B/op: 5,600 → 2,528）。ヒット件数が多い場合（300行）は `GetByKeysSorted` のバッチフェッチが支配的なため 3.3x。

---

### 5. LIKE 前方一致 (WHERE name LIKE 'name\_50%') — 11行ヒット

| データ量 | 条件 | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|------|---------------|-------------|-------------|
| 10,000行 | インデックスなし | 812,712 | 1,675,755 | **2.1x** |
| 10,000行 | インデックスあり | 19,742 | 29,421 | **1.5x** |

**考察:** `GetByKeysSorted` の疎キー最適化 + インラインページスキャン最適化 + `DecodeRowN` プリアロケーションにより **1.5x** を維持。11行ヒットではプライマリ B+Tree のバッチフェッチコストが支配的であり、DiskSecondaryBTree の検索コストは誤差の範囲。

---

### 6. 複合インデックス (WHERE category = X AND val >= Y)

| データ量 | 条件 | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|------|---------------|-------------|-------------|
| 10,000行 | インデックスなし | 715,824 | 1,531,426 | **2.1x** |
| 10,000行 | インデックスあり | 15,642 | 101,425 | **6.5x** |

**考察:** 疎キー最適化 + インラインページスキャン最適化 + `DecodeRowN` プリアロケーションで **6.5x**。50 キーのプライマリ B+Tree バッチフェッチが支配的であり、`findLeaf` のインライン化による改善（111,142 → 101,425 ns/op, 約 9%）は相対的に小さい。

---

### 7. ORDER BY + LIMIT

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|---------------|-------------|-------------|
| PK ASC + LIMIT 10 | 1,329 | 2,116 | **1.6x** |
| PK DESC + LIMIT 10 | 1,363 | 2,564 | **1.9x** |
| LIMIT 10 (ORDER BY なし) | 1,087 | 1,856 | **1.7x** |

**考察:** PK ASC + LIMIT は **1.6x** に改善（以前は 2.1x）。`findLeaf` / leftmost leaf descent のインライン化により、ツリー下降のアロケーションが排除された（B/op: 6,360 → 3,288）。PK DESC + LIMIT は `prevLeaf` バックポインタ + `findRightmostLeaf` のインライン化により **1.9x**（以前は 2.2x）。LIMIT 10（ORDER BY なし）は **1.7x**（以前は 2.2x）。

---

### 8. JOIN (1,000行)

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|---------------|-------------|-------------|
| WHERE なし + インデックスなし | 141,755 | 236,694 | **1.7x** |
| WHERE なし + インデックスあり | 95,897 | 115,429 | **1.2x** |
| 複合インデックス | 52,069 | 106,020 | **2.0x** |
| 個別インデックス | 78,074 | 101,513 | **1.3x** |
| LIMIT 10 | 11,659 | 20,334 | **1.7x** |

**考察:** Primary B+Tree のインラインページスキャン最適化 + `DecodeRowN` プリアロケーションにより JOIN 全般が改善。インデックスありの Hash Join パスが 1.2x と最も高速。LIMIT 10 では 1.7x。

---

### 9. DISTINCT + LIMIT

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|---------------|-------------|-------------|
| DISTINCT LIMIT 10 (改善前) | 235,948 | 1,350,039 | **5.7x** |
| DISTINCT LIMIT 10 (ScanEach 改善後) | 2,304 | 3,151 | **1.4x** |

**メモリ使用量 (10,000行、改善後):**
- Memory: 2,264 B/op, 62 allocs/op
- Disk: 3,280 B/op, 95 allocs/op

**考察:** `ScanEach` ストリーミング最適化により、DISTINCT + LIMIT が **5.7x → 1.4x** に大幅改善。`ScanEach` はテーブルロック保持中にコールバックをインラインで実行し、WHERE → projection → dedup → early exit を 1 パスで処理する。ユニーク値が limit 個集まった時点でスキャンを打ち切るため、disk ではデコードするページ数が大幅に削減される。Memory も `Scan()` 全行収集から `ScanEach` ストリーミングに切り替わったことで 235,948 → 2,304 ns/op（**102x 高速化**）。Disk は 1,350,039 → 3,151 ns/op（**428x 高速化**）。Primary B+Tree の `findLeaf` / `ForEach` のインライン化により Disk B/op も 6,352 → 3,280 に削減。安全性のため、WHERE/SELECT にサブクエリがある場合、JOIN、CTE、テーブルエイリアス、インデックススキャン使用時は従来の `filterProjectDedupLimit` パスにフォールスルーする。

---

### 10. WHERE + LIMIT (ScanEach ストリーミング拡張)

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory | Memory (B/op) | Disk (B/op) |
|---------|---------------|-------------|-------------|--------------|------------|
| WHERE + LIMIT 10 (改善前: Scan + filterWhereLimit) | 665,614 | 1,400,458 | **2.1x** | 1,005,472 | 2,130,391 |
| WHERE + LIMIT 10 (ScanEach 改善後) | 40,799 | 115,494 | **2.8x** | 1,944 | 94,905 |

**クエリ:** `SELECT * FROM bench WHERE category = 3 LIMIT 10`（10,000行、インデックスなし、100行ヒット中 10行取得）

**考察:** DISTINCT + LIMIT 向けに実装した `ScanEach` ストリーミングを non-DISTINCT の WHERE + LIMIT にも拡張。`executeScanEachStreaming` メソッドに `distinct bool` パラメータを追加し、ガード条件を `stmt.Distinct` → `(stmt.Distinct || stmt.Where != nil)` に拡張した。旧パスでは `Scan()` で全 10,000 行を `[]Row` に実体化してから `filterWhereLimit` で WHERE フィルタ + early exit していたが、新パスでは `ScanEach` コールバック内で WHERE → projection → early exit を 1 パスで処理する。Memory は 665,614 → 40,799 ns/op（**16x 高速化**）、Disk は 1,400,458 → 115,494 ns/op（**12x 高速化**）。メモリ使用量も Memory は 1,005,472 → 1,944 B/op（**517x 削減**）、Disk は 2,130,391 → 94,905 B/op（**22x 削減**）と大幅に改善した。

---

### 11. WHERE + LIMIT インデックスストリーミング（OrderedRangeScan）

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory | Memory (B/op) | Disk (B/op) |
|---------|---------------|-------------|-------------|--------------|------------|
| 等値 `WHERE category = 3 LIMIT 10` (改善前: バッチ) | 5,964 | 42,680 | **7.2x** | 6,623 | 39,804 |
| 等値 `WHERE category = 3 LIMIT 10` (ストリーミング改善後) | 5,983 | 5,531 | **~1.0x** | 2,896 | 4,552 |
| 範囲 `WHERE val > 5000 LIMIT 10` (改善前: バッチ) | 1,222,670 | 1,649,514 | **1.3x** | 946,785 | 4,013,682 |
| 範囲 `WHERE val > 5000 LIMIT 10` (ストリーミング改善後) | 2,880 | 5,183 | **1.8x** | 2,376 | 4,696 |
| ポストフィルタ `WHERE category = 3 AND val > 5000 LIMIT 10` (改善前: バッチ) | 6,723 | 45,324 | **6.7x** | 6,949 | 40,166 |
| ポストフィルタ `WHERE category = 3 AND val > 5000 LIMIT 10` (ストリーミング改善後) | 7,493 | 8,511 | **1.1x** | 3,256 | 6,192 |

**クエリ:** 10,000行、セカンダリインデックスあり（`CREATE INDEX` on `category`, `val`）

**考察:** `WHERE indexed_col = X LIMIT K` や `WHERE indexed_col > X LIMIT K` のようなクエリで、従来は `Lookup()` / `RangeScan()` が全マッチキーを収集 → `GetByKeys()` が全行フェッチ → `filterWhereLimit` で LIMIT 適用というバッチパスだったものを、`OrderedRangeScan` のコールバックベース走査で K 件に達した時点で早期打ち切りするストリーミングパスに変更。Primary B+Tree の `findLeaf` インライン化により、ストリーミング中の各 PK ルックアップのオーバーヘッドが大幅に削減された。

- **範囲検索が最も効果的**: `WHERE val > 5000` は 5,000 行がマッチするが、LIMIT 10 により 10 行だけフェッチすれば十分。Memory は 1,222,670 → 2,880 ns/op（**425x 高速化**）、Disk は 1,649,514 → 5,183 ns/op（**318x 高速化**）。メモリ使用量も Memory で 946,785 → 2,376 B/op（**399x 削減**）、Disk で 4,013,682 → 4,696 B/op（**855x 削減**）
- **等値検索**: category は 100 種類で各 100 行マッチのため改善幅は控えめだが、Disk では 42,680 → 5,531 ns/op（**7.7x 高速化**）。`findLeaf` インライン化により各 PK ルックアップの B/op が大幅に削減され、Disk と Memory がほぼ同等の **~1.0x** に到達
- **ポストフィルタ**: インデックス条件（`category = 3`）で候補を絞り込み、追加条件（`val > 500`）はコールバック内で全 WHERE を再評価。Disk で 45,324 → 8,511 ns/op（**5.3x 高速化**）。Disk/Memory は **1.1x** でほぼ Memory と同等
- **安全性**: PK（最大1行）、IN（複数 Lookup）、複合インデックスは既存バッチパスにフォールスルー。コールバック内で全 WHERE を評価するため、インデックスが部分的にしか WHERE をカバーしない場合も正しく動作

---

### 12. Covering Index（PK ルックアップスキップ）

セカンダリインデックスの composite key にはインデックス対象カラムの値がエンコード済みで格納されている。クエリが参照するカラムが全てインデックスカラム + PK で賄える場合（Covering Index）、PK ルックアップを完全にスキップし、composite key から直接カラム値をデコードして Row を構築する。

#### 12a. 等値検索 (`WHERE val = X`)

| パターン | Disk (ns/op) | Disk (B/op) | Disk (allocs/op) |
|---------|-------------|------------|------------------|
| Covering (`SELECT val`) | 1,764 | 1,354 | 34 |
| Non-Covering (`SELECT *`) | 2,322 | 1,826 | 46 |
| **改善** | **1.3x 高速化** | **26% 削減** | **26% 削減** |

#### 12b. 範囲検索 + LIMIT (`WHERE val > X LIMIT 10`)

| パターン | Disk (ns/op) | Disk (B/op) | Disk (allocs/op) |
|---------|-------------|------------|------------------|
| Covering (`SELECT val`) | 3,190 | 2,832 | 94 |
| Non-Covering (`SELECT *`) | 5,301 | 4,888 | 131 |
| **改善** | **1.7x 高速化** | **42% 削減** | **28% 削減** |

#### 12c. ORDER BY DESC + LIMIT (`ORDER BY val DESC LIMIT 10`)

| パターン | Disk (ns/op) | Disk (B/op) | Disk (allocs/op) |
|---------|-------------|------------|------------------|
| Covering (`SELECT val`) | 3,267 | 4,872 | 82 |
| Non-Covering (`SELECT *`) | 6,459 | 7,008 | 119 |
| **改善** | **2.0x 高速化** | **30% 削減** | **31% 削減** |

**考察:** Covering Index により、セカンダリインデックス経由クエリで PK ルックアップ（Primary B+Tree の `findLeaf` → リーフ → `DecodeRowN`）を完全にスキップし、composite key の `DecodeCompositeKeyValues` で直接 Row を構築する。PK ルックアップのコストが支配的なパスほど効果が大きく、ORDER BY DESC + LIMIT で **2.0x**、範囲 + LIMIT で **1.7x**、等値で **1.3x** の高速化。`CoveringIndexReader` インターフェースを型アサーションで利用し、既存パスとの互換性を維持。カラム依存性分析（`collectNeededColumns` / `isIndexCovering`）で自動的に covering 判定を行い、全ての SELECT/WHERE/ORDER BY で参照されるカラムがインデックスカラム + PK に含まれる場合のみ covering パスを使用する。

---

## まとめ

| カテゴリ | Disk/Memory 比率 | 評価 |
|---------|-----------------|------|
| PK ポイントルックアップ | 1.3x | 良好（Primary B+Tree インライン最適化で改善） |
| セカンダリインデックス ポイントルックアップ | 1.7x | 良好（Primary + Secondary インライン最適化で改善） |
| フルスキャン (インデックスなし) | 1.8x〜2.6x | 許容範囲 |
| インデックス利用 + 少数ヒット (≤3行) | 1.5x〜1.7x | 良好（インライン最適化で改善） |
| インデックス利用 + 多数ヒット (100行〜) | 1.3x〜6.5x | 良好 |
| IN インデックス (300行ヒット) | 3.3x | 許容範囲 |
| IN ユニーク (3行ヒット) | 1.5x | 良好（インライン最適化で改善） |
| LIKE インデックス (疎キー) | 1.5x | 良好 |
| ORDER BY PK ASC + LIMIT | 1.6x | 良好（インライン最適化で改善） |
| ORDER BY PK DESC + LIMIT | 1.9x | 良好（インライン最適化で改善） |
| LIMIT (ORDER BY なし) | 1.7x | 良好（インライン最適化で改善） |
| DISTINCT + LIMIT (ScanEach ストリーミング) | 1.4x | 良好（ScanEach + インライン最適化で改善） |
| WHERE + LIMIT (ScanEach ストリーミング) | 2.8x | 良好（ScanEach 最適化で改善） |
| WHERE + LIMIT インデックスストリーミング (等値) | ~1.0x | 良好（インライン最適化で Memory 同等に到達） |
| WHERE + LIMIT インデックスストリーミング (範囲) | 1.8x | 良好（インライン最適化で改善） |
| JOIN (Hash Join) | 1.2x〜1.7x | 良好 |
| JOIN (インデックス利用) | 1.3x〜2.0x | 良好 |
| Covering Index 等値検索 | 1.3x 高速化 (vs Non-Covering) | 良好（PK lookup スキップ） |
| Covering Index 範囲 + LIMIT | 1.7x 高速化 (vs Non-Covering) | 良好（PK lookup スキップ） |
| Covering Index ORDER BY + LIMIT | 2.0x 高速化 (vs Non-Covering) | 良好（PK lookup スキップ） |
| PK Covering ORDER BY + LIMIT | 1.2x 高速化 (vs Full Row)、Disk/Memory 1.1x | 良好（行デコードスキップ） |
| PK ORDER BY + WHERE (ScanEachWithKey) | 2.9x | 良好（DecodeRowInto + ScanEachWithKey で Disk B/op 8.1x 削減） |
| バッチインデックス + ポストフィルタ (LIMIT なし) | 3.2x | 許容範囲（Bulk Slab で Row alloc N+1 → 2） |
| バッチインデックス + ポストフィルタ + LIMIT (ForEachByKeys) | 2.1x | 良好（ForEachByKeys ストリーミングで LIMIT 早期打ち切り） |

### 主要な知見

1. **PK ポイントルックアップの基本オーバーヘッドは 1.3x**: Primary B+Tree の `findLeaf` でページバッファ上の直接オフセット計算による inline binary search を行い、`decodeInternalPage` の `make([]int64, n)` + `make([]uint32, n+1)` アロケーションを完全に排除。B/op が 4,280 → 1,208（**3.5x 削減**）。バッファプールがキャッシュ済みの場合、メモリストレージの Go ポインタ直接参照に比べて約 1.3 倍であり、実用上のオーバーヘッドは最小限。

2. **Primary B+Tree のインラインページスキャン最適化**: `findLeaf` (binary search → child)、`ForEach` / `ForEachRange` (leftmost child descent)、`findRightmostLeaf` (rightmost child descent) の 4 箇所で `decodeInternalPage` を排除し、ページバッファ上の直接オフセット計算に置換。これにより PK ルックアップは 2.3x → **1.3x**、セカンダリインデックスルックアップ（PK ルックアップ含む）は 2.4x → **1.7x** に改善。Secondary B-Tree の既存インライン最適化と合わせ、読み取りパス全体でアロケーションフリーのツリー下降を実現。

3. **セカンダリインデックスのインラインページスキャン最適化**: DiskSecondaryBTree の読み取り系メソッド（`findLeaf`, `PrefixScan`, `RangeScan`, `ForEach` 等）でページバッファ上の直接走査を行い、`decodeSecLeafPage` / `decodeSecInternalPage` の全エントリ分 `make+copy` を排除。これによりポイントルックアップは 6.4x → **1.7x** に段階的に改善（Secondary インライン: 6.4x → 2.4x、Primary インライン: 2.4x → 1.7x）。

4. **`DecodeRowN` プリアロケーション最適化**: `DecodeRow` にカラム数を事前に渡す `DecodeRowN` を導入し、DiskBTree のホットパス（`Get`, `GetByKeysSorted`, `ForEach`, `ForEachReverse`, `ForEachRange`）で Row スライスの `append` による再割り当てを排除。全ベンチマークで一貫して allocs/op が 2 削減、B/op が 48 削減された。

5. **セカンダリインデックスからの多数行フェッチは改善済み**: `GetByKeys` のバッチフェッチ最適化により、rowKey をソートしてリーフチェーンを 1 回のシーケンシャルスキャンで処理する方式に変更。疎キー最適化（ヘッダースキップ + ギャップジャンプ）と部分デコード最適化（ページ内インライン走査 + マッチ時のみ DecodeRowN）を組み合わせ、範囲検索は **1.3x**、LIKE は **1.5x**。

6. **`ForEachReverse` は `prevLeaf` バックポインタで ASC 同等に改善済み**: リーフページヘッダに `prevLeaf` (4B) バックポインタを追加し、`findRightmostLeaf` (O(H)) + `prevLeaf` チェーン逆方向走査に変更。ORDER BY PK DESC + LIMIT が **1.9x** で ASC (1.6x) とほぼ同等の水準。

7. **LIMIT (ORDER BY なし) は `earlyLimit` 伝搬で改善済み**: `scanSourceSingle` が WHERE なし + LIMIT の場合に `ScanOrdered(name, false, limit)` を呼ぶことで **1.7x**。DISTINCT + LIMIT も `ScanEach` ストリーミング + インライン最適化により **5.7x → 1.4x** に改善済み（後述 9 を参照）。

8. **Hash Join は disk で最も効率的な JOIN パス**: inner テーブルのフルスキャン 1 回でハッシュテーブルを構築するため、個別 Get の積算が発生せず 1.2〜1.7x に留まる。

9. **`ScanEach` ストリーミングで DISTINCT + LIMIT が 5.7x → 1.4x に改善**: `ScanEach` メソッドをストレージインターフェースに追加し、テーブルロック保持中にコールバックをインラインで実行する方式に変更。`ForEachRow` の二段階収集（ロック下で全行をスライスに収集 → ロック解放後にコールバック）と異なり、コールバック内で WHERE → projection → dedup → early exit を 1 パスで処理する。disk ではユニーク値が揃った時点でページデコードを打ち切るため、デコードするページ数が大幅に削減された。Primary B+Tree のインライン最適化により B/op も 6,352 → 3,280 に削減。安全性のため、サブクエリ・JOIN・CTE・テーブルエイリアス・インデックススキャン使用時は従来パスにフォールスルーする。

10. **`ScanEach` ストリーミングを WHERE + LIMIT にも拡張**: `executeScanEachStreaming` に `distinct bool` パラメータを追加し、ガード条件を `(stmt.Distinct || stmt.Where != nil)` に拡張。non-DISTINCT の WHERE + LIMIT でも `Scan()` 全行実体化を回避し、`ScanEach` コールバック内で WHERE → projection → early exit を 1 パスで処理。Memory で **16x**、Disk で **12x** の高速化、メモリ使用量も Memory で **517x**、Disk で **22x** 削減。

11. **WHERE + LIMIT のインデックスストリーミングで Disk が Memory 同等に到達**: `OrderedRangeScan` のコールバックベース走査を `executeIndexScanStreaming` で再利用し、LIMIT 件数に達した時点でインデックス走査を早期打ち切り。Primary B+Tree の `findLeaf` インライン化により各 PK ルックアップのアロケーションが大幅に削減された結果、等値検索で Disk/Memory **~1.0x**、ポストフィルタで **1.1x** とほぼ Memory 同等の性能に到達。範囲検索（`WHERE val > 5000 LIMIT 10`、5,000行マッチ）でも Disk **318x** 高速化（1,649,514 → 5,183 ns/op）、B/op **855x** 削減（4,013,682 → 4,696）。

12. **Covering Index で PK ルックアップを完全にスキップ**: セカンダリインデックスの composite key からカラム値を直接デコードする `DecodeCompositeKeyValues` を導入し、クエリが参照する全カラムがインデックスカラム + PK に含まれる場合に PK ルックアップを排除。`CoveringIndexReader` インターフェースの `LookupCovering` / `OrderedCoveringScan` メソッドで実装。カラム依存性分析（`collectNeededColumns` + `isIndexCovering`）により自動的に covering 判定。等値で **1.3x**、範囲 + LIMIT で **1.7x**、ORDER BY + LIMIT で **2.0x** の高速化（Non-Covering 比）。メモリ使用量も 26〜42% 削減。

13. **PK Covering Index で行デコードを完全にスキップ**: PK カラムのみを参照するクエリ（`SELECT id` や `COUNT(*)`）で、Primary B+Tree のリーフエントリからキー（int64）を直接読み取り、バリュー領域の `DecodeRowN` を完全にスキップ。`ForEachKeyOnly` / `ForEachKeyOnlyReverse` メソッドでリーフページのバリュー領域を `pos += valLen` で飛ばす。Disk で ORDER BY PK + LIMIT が **1.2x 高速化**（1,873 vs 2,273 ns/op）、B/op **28% 削減**（2,496 vs 3,480）。Disk/Memory 比率も Full Row の 1.5x から PK Covering の **1.1x** に改善され、ほぼ Memory 同等に到達。`collectColumnRefs` の `COUNT(*)` 特殊処理（`CallExpr` 内の `StarExpr` をスキップ）により、`COUNT(*)` も自動的に PK Covering の対象となる。

14. **`DecodeRowInto` + `ScanEachWithKey` で PK ORDER BY + WHERE の B/op 8.1x 削減**: `DecodeRowInto` で Row スライスのバッキング配列を再利用し、フィルタで除外される行の `make(Row, 0, numCols)` を排除。`ScanEachWithKey` でインラインコールバック実行し、`ForEachRow` の全行 `[]Row` 収集を回避。WHERE selectivity 0.1%（10,000行中10行マッチ）で Disk B/op が 4,589,329 → 566,256（**8.1x 削減**）、allocs/op が 60,013 → 50,015（**9,998 allocs 削減**）。Memory も B/op が 3,467,513 → 84,392（**41x 削減**）。DiskBTree の `ForEachReuse` / `ForEachReverseReuse` は `DecodeRowInto` を使い、既存 `ScanEach` のコールバック規約と整合する形で Row 再利用を実現。

15. **`GetByKeysSorted` Bulk Slab + `ForEachByKeys` ストリーミングで バッチインデックスパスを最適化**: 2 つの最適化を組み合わせ。(A) `GetByKeysSorted` で `make([]Value, len(sortedKeys)*numCols)` の flat 配列を事前確保し、三要素スライス式 `slab[i:i+n:i+n]` で各 Row を切り出す。`DecodeRowDirect` でインデックス書き込みを使い、N 個の Row alloc → 1 slab alloc に削減。(B) `ForEachByKeysSorted` + `ForEachByKeys` インターフェースで、`DecodeRowInto` による Row 再利用のストリーミングパスを提供。ストリーミング高速パスで `tryIndexScan` が keys を返す場合に `executeForEachByKeysStreaming` を使い、バッチパスへのフォールスルーを排除。LIMIT 付きクエリで Disk 52,025 → 28,361 ns/op（**1.8x 高速化**）、B/op 42,670 → 16,567（**2.6x 削減**）。

### 13. PK Covering Index（行デコードスキップ）

PK カラムのみを参照するクエリ（`SELECT id FROM t ORDER BY id LIMIT 10` や `SELECT COUNT(*) FROM t`）では、Primary B+Tree のリーフエントリからキー（int64）を直接読み取り、バリュー領域の `DecodeRowN` を完全にスキップする。セカンダリ Covering Index（`DecodeCompositeKeyValues`）と異なり、PK Covering は**デコードコストがゼロ**（キーが直接使える）なので、Disk ストレージでも効果が得られる。

`collectColumnRefs` の `CallExpr` 内 `StarExpr` 処理も修正し、`COUNT(*)` を PK Covering の対象にした（集約関数内の `*` は全カラムデータを必要としない）。

#### 13a. ORDER BY PK ASC + LIMIT 10 (`SELECT id` vs `SELECT *`)

| パターン | Disk (ns/op) | Disk (B/op) | Disk (allocs/op) |
|---------|-------------|------------|------------------|
| PK Covering (`SELECT id ORDER BY id LIMIT 10`) | 1,873 | 2,496 | 47 |
| Full Row (`SELECT * ORDER BY id LIMIT 10`) | 2,273 | 3,480 | 64 |
| **改善** | **1.2x 高速化** | **28% 削減** | **27% 削減** |

#### 13b. Memory vs Disk 比較

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory |
|---------|---------------|-------------|-------------|
| PK Covering (`SELECT id ORDER BY id LIMIT 10`) | 1,756 | 1,873 | **1.1x** |
| Full Row (`SELECT * ORDER BY id LIMIT 10`) | 1,528 | 2,273 | **1.5x** |

**考察:** Disk ストレージでは PK Covering により ORDER BY PK ASC + LIMIT が **1.2x 高速化**、B/op **28% 削減**、allocs/op **27% 削減**。`ForEachKeyOnly` でリーフページのバリュー領域を `pos += valLen` でスキップし、`DecodeRowN` のアロケーション（Row スライス割り当て + 各カラム値のデコード）を完全に排除した。Disk/Memory 比率も Full Row の **1.5x** から PK Covering の **1.1x** に改善され、ほぼ Memory 同等の性能に到達した。これは PK Covering パスではバッファプール経由のページフェッチのオーバーヘッドのみが残り、`DecodeRowN` の差分が消えるためである。

メモリストレージでは PK Covering (1,756 ns/op) が Full Row (1,528 ns/op) より微増する。メモリ上の行アクセスが Go ポインタ直接参照で非常に高速なため、`isPKOnlyCovering` の判定 + `buildPKOnlyRow` の Row 構築オーバーヘッドが行コピーの節約分を上回る。ただし B/op は 2,352 vs 2,456 と **4% 削減**される。

---

### 14. PK ORDER BY + WHERE フィルタ（ScanEachWithKey + DecodeRowInto）

PK ORDER BY + WHERE 条件付きクエリで、`ScanEachWithKey` によるインライン実行と `DecodeRowInto` による Row スライス再利用を組み合わせ、フィルタで除外される行のメモリ割り当てを削減する。従来の `ForEachRow` パスでは全行を `[]Row` スライスに収集した後で WHERE フィルタを適用していたが、新パスではコールバック内で WHERE 評価 → マッチ時のみ Row コピーという 1 パス処理を行う。

**クエリ:** `SELECT * FROM bench WHERE id > 9990 ORDER BY id`（10,000行中 10行マッチ = 0.1% selectivity）

#### 14a. ORDER BY PK + WHERE vs 全行取得

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory | Memory (B/op) | Disk (B/op) |
|---------|---------------|-------------|-------------|--------------|------------|
| ORDER BY PK 全行 (ベースライン) | 898,000 | 1,793,000 | **2.0x** | 3,467,513 | 4,589,329 |
| ORDER BY PK + WHERE (0.1% hit) | 332,000 | 952,000 | **2.9x** | 84,392 | 566,256 |
| ORDER BY PK DESC + WHERE (0.1% hit) | 337,000 | 961,000 | **2.9x** | 84,392 | 566,704 |

#### 14b. メモリ使用量比較

| | Memory (B/op) | Memory (allocs) | Disk (B/op) | Disk (allocs) |
|--|--------------|----------------|------------|--------------|
| 全行 (ベースライン) | 3,467,513 | 10,044 | 4,589,329 | 60,013 |
| WHERE 0.1% hit | 84,392 | 10,045 | 566,256 | 50,015 |
| **削減** | **41x** | — | **8.1x** | **9,998 allocs 削減** |

**考察:**

- **Disk B/op 8.1x 削減**: `DecodeRowInto` が Row スライスのバッキング配列を再利用するため、フィルタで除外される 9,990 行分の `make(Row, 0, numCols)` が排除される。allocs/op も 60,013 → 50,015（**9,998 allocs 削減**）で、ほぼ Row スライス分の削減に一致
- **Disk 1.9x 高速化**: 全行取得 (1,793,000 ns/op) と比較して WHERE 0.1% hit は 952,000 ns/op。全行のページデコード自体は依然必要（リーフチェーン走査は変わらない）だが、Row スライスの alloc/GC 圧力が大幅に軽減
- **Memory B/op 41x 削減**: Memory ストレージでもインライン実行により `[]Row` 全行収集が不要に。Memory は元々デコード不要なため allocs/op はほぼ変化なし (10,044 → 10,045)
- **Disk/Memory 2.9x**: Disk が全行のページデコードを依然実行するのに対し、Memory ではデコード不要でインライン WHERE 評価のみのため、全行取得 (2.0x) より比率は劣る。ただし Disk の 952,000 ns/op は全行 1,793,000 ns/op の **1.9x 高速化**であり、WHERE 早期打ち切りの恩恵を受ける
- **ASC/DESC ほぼ同等**: `ForEachReverseReuse` も `prevLeaf` バックポインタで逆方向走査しつつ `DecodeRowInto` を適用。DESC (961,000 ns/op) と ASC (952,000 ns/op) はほぼ同等

---

### 15. GetByKeysSorted Bulk Slab 割り当て + ForEachByKeys ストリーミング

`GetByKeysSorted` の Row alloc を Bulk Slab 化し、`ForEachByKeys` コールバックで Row 再利用によるストリーミングパスを追加。

#### A. Bulk Slab 割り当て（GetByKeysSorted 内部最適化）

`GetByKeysSorted` で事前に `make([]Value, len(sortedKeys) * numCols)` の flat 配列を確保し、マッチごとに三要素スライス式 `slab[i:i+n:i+n]` で Row を切り出す。N 個の Row alloc → 1 alloc に削減。`DecodeRowDirect` でインデックス書き込み（`dst[idx] = value`）を使い `append` を排除。

#### B. ForEachByKeys コールバック（ストリーミング + Row 再利用）

`ForEachByKeysSorted` で `DecodeRowInto` により 1 つの Row バッファを使い回し、コールバック内で WHERE 評価。フィルタで除外される行は alloc ゼロ。ストリーミング高速パスで `tryIndexScan` が keys を返す場合に `executeForEachByKeysStreaming` でストリーミング実行し、バッチパスへのフォールスルーを排除。

#### 15a. バッチインデックス + ポストフィルタ（LIMIT なし）

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory | Memory (B/op) | Disk (B/op) |
|---------|---------------|-------------|-------------|--------------|------------|
| `WHERE category = 3 AND val > 50000` | 16,199 | 52,025 | **3.2x** | 13,759 | 42,670 |

**クエリ:** 10,000行、インデックスあり（category + composite）。category = 3 で ~100行絞り込み → val > 50000 でさらに ~50行。

#### 15b. バッチインデックス + ポストフィルタ + LIMIT（ForEachByKeys ストリーミング）

| パターン | Memory (ns/op) | Disk (ns/op) | Disk/Memory | Memory (B/op) | Disk (B/op) |
|---------|---------------|-------------|-------------|--------------|------------|
| `WHERE category = 3 AND val > 50000 LIMIT 5` | 13,319 | 28,361 | **2.1x** | 3,336 | 16,567 |

**考察:**

- LIMIT 5 追加で Disk は 52,025 → 28,361 ns/op（**1.8x 高速化**）、B/op は 42,670 → 16,567（**2.6x 削減**）。`ForEachByKeys` ストリーミングにより LIMIT 件数に達した時点で Row デコードを早期打ち切り
- Memory も 16,199 → 13,319 ns/op（**1.2x 高速化**）、B/op は 13,759 → 3,336（**4.1x 削減**）。Memory では Row 再利用は不要だが、LIMIT による早期打ち切りの効果を享受
- Disk の Bulk Slab により `GetByKeysSorted`（LIMIT なしパス）の Row alloc が N+1 → 2 に削減。300行 IN や複合インデックス 50行のケースで GC 圧力を軽減

---

## 改善優先度

| 改善項目 | 影響 | 難易度 |
|---------|------|-------|
| ~~`ForEachReverse` をリーフ逆順走査に変更~~ | **改善済み (1.9x)** — `prevLeaf` バックポインタ追加 | — |
| ~~`GetByKeys` のバッチフェッチ最適化~~ | **改善済み (範囲: 1.3x, IN: 3.3x)** | — |
| ~~`GetByKeysSorted` 疎キー + 部分デコード最適化~~ | **改善済み (LIKE: 1.5x, 複合: 6.5x)** | — |
| ~~`Scan()` の早期打ち切り対応~~ | **改善済み (1.7x)** — `earlyLimit` を `ScanOrdered` に伝搬 | — |
| ~~セカンダリインデックスのディスク永続化~~ | **実施済み** — 起動時再構築不要 | — |
| ~~DiskSecondaryBTree のインラインページスキャン最適化~~ | **改善済み (6.4x → 1.7x)** — 読み取り系メソッドでページバッファ直接走査 | — |
| ~~Primary B+Tree の `findLeaf` インラインページスキャン最適化~~ | **改善済み (PK: 2.3x → 1.3x, B/op: 4,280 → 1,208)** — `decodeInternalPage` 排除 | — |
| ~~`DecodeRowN` プリアロケーション最適化~~ | **改善済み** — 全パスで allocs/op -2, B/op -48 | — |
| ~~`ScanEach` ストリーミング (DISTINCT + LIMIT 向け)~~ | **改善済み (5.7x → 1.4x)** — `ScanEach` で inline callback + early exit | — |
| ~~`ScanEach` ストリーミングを WHERE + LIMIT に拡張~~ | **改善済み (Memory 16x, Disk 12x 高速化)** — `executeScanEachStreaming` を汎用化 | — |
| ~~WHERE + LIMIT のインデックスストリーミング~~ | **改善済み (等値: ~1.0x, 範囲: Disk 318x 高速化)** — `OrderedRangeScan` + `findLeaf` インライン化 | — |
| ~~Covering Index（PK ルックアップスキップ）~~ | **改善済み (等値: 1.3x, 範囲: 1.7x, ORDER BY: 2.0x)** — `DecodeCompositeKeyValues` + `CoveringIndexReader` | — |
| ~~PK Covering Index（行デコードスキップ）~~ | **改善済み (Disk: 1.2x 高速化, Disk/Memory: 1.1x)** — `ForEachKeyOnly` + `COUNT(*)` 特殊処理 | — |
| ~~`DecodeRowInto` + `ScanEachWithKey` (PK ORDER BY + WHERE)~~ | **改善済み (Disk B/op 8.1x 削減, allocs -9,998)** — Row スライス再利用 + インラインコールバック | — |
| ~~`GetByKeysSorted` Bulk Slab + `ForEachByKeys` ストリーミング~~ | **改善済み (Slab: Row alloc N+1→2, ForEachByKeys+LIMIT: Disk 1.8x 高速化)** — `DecodeRowDirect` + 三要素スライス式 + コールバック Row 再利用 | — |
