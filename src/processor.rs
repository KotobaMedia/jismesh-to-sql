use anyhow::{Context, Result};
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use derive_builder::Builder;
use futures_util::TryFutureExt;
use indicatif::{ProgressBar, ProgressStyle};
use ndarray::{Array1, Axis};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_postgres::NoTls;
use url::Url;

use crate::schema;

const TABLE_NAME: &str = "jismesh_codes";
const TRANSACTION_SIZE: usize = 5_000;

pub async fn init_db(postgres_url: &str) -> Result<deadpool_postgres::Pool> {
    let mut cfg = Config::new();
    cfg.url = Some(postgres_url.to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .with_context(|| "when initializing DB pool")?;
    {
        let client = pool.get().await?;
        schema::init_schema(&client)
            .await
            .with_context(|| "when initializing DB schema")?;
    }
    Ok(pool)
}

#[derive(Debug, Clone)]
struct MeshcodeRow {
    code: u64,
    level: usize,
    xmin: f64, // min longitude, west
    ymin: f64, // min latitude, south
    xmax: f64, // max longitude, east
    ymax: f64, // max latitude, north
}

fn generate_codes(
    tx: async_channel::Sender<MeshcodeRow>,
    p_tx: tokio::sync::mpsc::Sender<ProgressMsg>,
    root_meshes: &[u64],
    levels: &[jismesh::MeshLevel],
) -> Result<()> {
    for root_mesh in root_meshes {
        for level in levels {
            let meshes = jismesh::to_intersects(*root_mesh, *level)?;
            let mesh_count = meshes.len();
            let multiplier_sw = Array1::from_elem(mesh_count, 0.0);
            let min_points =
                jismesh::to_meshpoint(meshes.clone(), multiplier_sw.clone(), multiplier_sw)?;
            let multiplier_ne = Array1::from_elem(mesh_count, 1.0);
            let max_points =
                jismesh::to_meshpoint(meshes.clone(), multiplier_ne.clone(), multiplier_ne)?;

            // println!("min: {}", min_points);
            // println!("max: {}", max_points);
            // min_points and max_points are in the shape [[lat1, lat2, lat3,...], [lon1, lon2, lon3, ...]]
            // let's zip them all together so it's [[xmin, ymin, xmax, ymax], ...]
            let bounding_boxes: Vec<[f64; 4]> = min_points
                .axis_iter(Axis(1))
                .zip(max_points.axis_iter(Axis(1)))
                .map(|(min_col, max_col)| {
                    // min_col[1] is xmin, min_col[0] is ymin
                    // max_col[1] is xmax, max_col[0] is ymax
                    [min_col[1], min_col[0], max_col[1], max_col[0]]
                })
                .collect();
            // println!("bounding boxes: {:?}", bounding_boxes);

            let rows = meshes
                .iter()
                .zip(bounding_boxes.iter())
                .map(|(mesh, bbox)| MeshcodeRow {
                    code: *mesh,
                    level: *level as usize,
                    xmin: bbox[0],
                    ymin: bbox[1],
                    xmax: bbox[2],
                    ymax: bbox[3],
                });
            p_tx.blocking_send(ProgressMsg::Count(mesh_count))?;
            for row in rows {
                // println!("row: {:?}", row);
                tx.send_blocking(row)
                    .with_context(|| "when sending mesh code row")?;
            }
        }
    }
    Ok(())
}

async fn join_all_inserters(mut inserters: JoinSet<Result<()>>) -> Result<()> {
    while let Some(join_result) = inserters.join_next().await {
        // Return early on error
        let _ = join_result??;
    }
    Ok(())
}

enum ProgressMsg {
    Count(usize),
    Progress(usize),
}

#[derive(Builder)]
pub struct Processor {
    pool: deadpool_postgres::Pool,
    root_meshes: Vec<u64>,
    levels: Vec<jismesh::MeshLevel>,
    skip_metadata: bool,
}

impl Processor {
    pub async fn process(&mut self) -> Result<()> {
        self.process_data().await?;
        if !self.skip_metadata {
            self.process_metadata().await?;
        }
        Ok(())
    }

    async fn process_data(&mut self) -> Result<()> {
        // given root meshes and levels, we'll generate polygons for each mesh
        // and insert them in to the database.
        // Generating polygons is done by jismesh crate, and is blocking, so we'll
        // start up a tokio blocking task to do this.
        // It's then sent through a channel to the database insert pool.
        let (tx, rx) = async_channel::bounded::<MeshcodeRow>(10_000);
        let (p_tx, mut p_rx) = tokio::sync::mpsc::channel::<ProgressMsg>(1000);
        let progress = tokio::task::spawn(async move {
            let bar = ProgressBar::new(0);
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}",
                )
                .unwrap()
                .progress_chars("=>-"),
            );
            bar.enable_steady_tick(Duration::from_millis(100));

            while let Some(msg) = p_rx.recv().await {
                match msg {
                    ProgressMsg::Count(c) => {
                        bar.inc_length(c as u64);
                    }
                    ProgressMsg::Progress(p) => {
                        bar.inc(p as u64);
                    }
                }
            }
        })
        .map_err(anyhow::Error::from);

        let root_meshes = self.root_meshes.clone();
        let levels = self.levels.clone();
        let gen_p_tx = p_tx.clone();
        let generator = tokio::task::spawn_blocking(move || -> Result<()> {
            println!("Generating mesh codes...");
            generate_codes(tx, gen_p_tx, &root_meshes, &levels)
                .with_context(|| "when generating mesh codes")?;
            Ok(())
        })
        .map_err(anyhow::Error::from);

        let inserters = self.setup_inserters(rx, p_tx)?;
        let join_inserters_future = join_all_inserters(inserters);
        let (_inserters_result, generator_result, _progress_result) =
            tokio::try_join!(join_inserters_future, generator, progress)?;

        generator_result.with_context(|| "when generating mesh codes")?;

        println!("All mesh codes generated and inserted.");
        Ok(())
    }

    async fn query_levels_from_db(&self) -> Result<Vec<jismesh::MeshLevel>> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare("SELECT DISTINCT level FROM jismesh_codes")
            .await?;
        let rows = client.query(&stmt, &[]).await?;
        let mut levels = Vec::new();
        for row in rows {
            let level: i32 = row.get(0);
            levels.push(jismesh::MeshLevel::try_from(level as usize)?);
        }
        Ok(levels)
    }

    async fn process_metadata(&mut self) -> Result<()> {
        use km_to_sql::{
            metadata::{ColumnEnumDetails, ColumnMetadata, TableMetadata},
            postgres::{init_schema, upsert},
        };

        let enum_values: Vec<ColumnEnumDetails> = self
            .query_levels_from_db()
            .await?
            .iter()
            .map(|l| ColumnEnumDetails {
                value: (*l as usize).to_string(),
                desc: Some(format!("{} ({})", l.to_string_jp(), l.to_size_jp())),
            })
            .collect();

        let d = TableMetadata {
            name: "メッシュコード位置参照".into(),
            desc: Some(
                "JIS X 0410 地域メッシュコードとgeometryを双方にマッピングするためのテーブル"
                    .into(),
            ),
            source: None,
            source_url: Some(
                Url::parse("https://www.stat.go.jp/data/mesh/pdf/gaiyo1.pdf").unwrap(),
            ),
            license: None,
            license_url: None,
            primary_key: Some("code".into()),
            columns: vec![
                ColumnMetadata {
                    name: "code".into(),
                    desc: Some("地域メッシュコード".into()),
                    data_type: "bigint".into(),
                    foreign_key: None,
                    enum_values: None,
                },
                ColumnMetadata {
                    name: "level".into(),
                    desc: Some("メッシュ区画 (1次、2次など)".into()),
                    data_type: "integer".into(),
                    foreign_key: None,
                    enum_values: Some(enum_values),
                },
                ColumnMetadata {
                    name: "geom".into(),
                    desc: Some("地域メッシュを表すポリゴン".into()),
                    data_type: "geometry(polygon, 4326)".into(),
                    foreign_key: None,
                    enum_values: None,
                },
            ],
        };
        let client = self.pool.get().await?;
        init_schema(&client)
            .await
            .with_context(|| "when initializing metadata schema")?;
        upsert(&client, TABLE_NAME, &d)
            .await
            .with_context(|| "when inserting metadata")?;

        Ok(())
    }

    fn setup_inserters(
        &self,
        rx: async_channel::Receiver<MeshcodeRow>,
        p_tx: tokio::sync::mpsc::Sender<ProgressMsg>,
    ) -> Result<JoinSet<Result<()>>> {
        let pool = self.pool.clone();
        let mut join_set = JoinSet::new();
        let max_tasks = num_cpus::get() * 4;
        for _ in 0..max_tasks {
            let pool = pool.clone();
            let rx = rx.clone();
            let p_tx = p_tx.clone();
            join_set.spawn(async move {
                let client = pool.get().await?;
                let begin = client.prepare("BEGIN").await?;
                let commit = client.prepare("COMMIT").await?;
                let stmt = client.prepare_cached(&format!(
                    "INSERT INTO {} (code, level, geom) VALUES ($1, $2, ST_MakeEnvelope($3, $4, $5, $6, 4326)) ON CONFLICT (code) DO NOTHING",
                    TABLE_NAME
                )).await?;
                let mut count: usize = 0;
                client.query(&begin, &[]).await?;
                while let Ok(row) = rx.recv().await {
                    // println!("inserter: {:?}", &row);
                    let code: i64 = row.code.try_into()?;
                    let level: i32 = row.level.try_into()?;
                    client
                        .query(
                            &stmt,
                            &[&code, &level, &row.xmin, &row.ymin, &row.xmax, &row.ymax],
                        )
                        .await?;
                    count += 1;
                    // p_tx.send(ProgressMsg::Progress(1)).await?;

                    if count % TRANSACTION_SIZE == 0 {
                        client.query(&commit, &[]).await?;
                        p_tx.send(ProgressMsg::Progress(TRANSACTION_SIZE)).await?;
                        client.query(&begin, &[]).await?;
                    }
                }
                client.query(&commit, &[]).await?;
                p_tx.send(ProgressMsg::Count(count % TRANSACTION_SIZE)).await?;
                Ok(())
            });
        }
        Ok(join_set)
    }
}
