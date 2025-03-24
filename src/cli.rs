use clap::Parser;

#[derive(Parser, Debug)]
pub struct Cli {
    /// Postgresデータベースに接続する文字列
    pub postgres_url: String,

    /// メタデータテーブルの更新・作成をスキップする。
    #[arg(long, default_value = "false")]
    pub skip_metadata: bool,

    /// カンマ区切りの整数のリスト。メッシュコードのテーブルを作成する際に、指定したレベルのメッシュコードを作成します。
    /// 受付可能な値は <https://docs.rs/jismesh/latest/jismesh/enum.MeshLevel.html> で参照できます。
    /// 数字または `Lv1` `X20` などの文字列を受付可能です。
    /// デフォルトでは、すべてのレベルのメッシュコードが対象となります。
    #[arg(short, long, value_delimiter = ',')]
    pub levels: Option<Vec<String>>,

    /// メッシュコードのテーブルを作成する際に、こちらのオプションで指定したメッシュコードの中の、 `levels` で指定したレベルのメッシュコードを作成します。
    /// デフォルトでは、日本陸地を表すメッシュコードすべてが対象となります。
    /// 詳しくは <https://docs.rs/jismesh/latest/jismesh/codes/constant.JAPAN_LV1.html> を参照してください。
    #[arg(short, long, value_delimiter = ',')]
    pub root_meshes: Option<Vec<u64>>,
}
