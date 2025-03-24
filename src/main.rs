use anyhow::Result;
use clap::Parser;
use strum::IntoEnumIterator;
use tokio;

mod cli;
mod processor;
mod schema;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::Cli::parse();
    // println!("{:?}", &args);
    let root_meshes = args
        .root_meshes
        .unwrap_or_else(|| jismesh::codes::JAPAN_LV1.to_vec());
    let mut levels = args
        .levels
        .unwrap_or_else(|| vec![])
        .iter()
        .map(|level| {
            if let Ok(level) = level.parse::<usize>() {
                jismesh::MeshLevel::try_from(level)
                    .unwrap_or_else(|_| panic!("Invalid mesh level: {}", level))
            } else {
                jismesh::MeshLevel::try_from(level.as_str())
                    .unwrap_or_else(|_| panic!("Invalid mesh level: {}", level))
            }
        })
        .collect::<Vec<_>>();
    if levels.is_empty() {
        levels.extend(jismesh::MeshLevel::iter());
    }

    let pool = processor::init_db(&args.postgres_url).await?;

    let mut processor = processor::ProcessorBuilder::default()
        .pool(pool)
        .root_meshes(root_meshes)
        .levels(levels)
        .skip_metadata(args.skip_metadata)
        .build()?;
    processor.process().await?;

    // println!("Root meshes: {:?}", root_meshes);
    // println!("Levels: {:?}", levels);
    Ok(())
}
