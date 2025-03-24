# jismesh-to-sql

このアプリケーションは KotobaMedia の `to-sql` シリーズの一つです。 jismesh-to-sql は JIS X0410 地域メッシュコードの参照テーブルを作る目的に作っています。

## 使い方

```
Usage: jismesh-to-sql [OPTIONS] <POSTGRES_URL>

Arguments:
  <POSTGRES_URL>  Postgresデータベースに接続する文字列

Options:
      --skip-metadata              メタデータテーブルの更新・作成をスキップする。
  -l, --levels <LEVELS>            カンマ区切りの整数のリスト。メッシュコードのテーブルを作成する際に、指定したレベルの メッシュコードを作成します。 受付可能な値は <https://docs.rs/jismesh/latest/jismesh/enum.MeshLevel.html> で参照できます 。
  -r, --root-meshes <ROOT_MESHES>  メッシュコードのテーブルを作成する際に、こちらのオプションで指定したメッシュコードの 中の、 `levels` で指定したレベルのメッシュコードを作成します。 デフォルトでは、日本陸地を表すメッシュコードすべてが対象 となります。 詳しくは <https://docs.rs/jismesh/latest/jismesh/codes/constant.JAPAN_LV1.html> を参照してください。
```
