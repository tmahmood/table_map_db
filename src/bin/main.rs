use std::path::{Path, PathBuf};
use std::time::Instant;
use rand::{Rng, thread_rng};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use table_map_db::{dump_csv, dump_db, TableMapDb};

pub fn generate_random_str(length: usize) -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(&rand::distributions::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/// Sets up simple enviornment based tracing.
/// DO NOT call from library. Always from bin
pub fn set_tracing() -> Result<(), anyhow::Error> {
    let subscriber = FmtSubscriber::builder()
        .compact()
        .with_line_number(true)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}


#[tokio::main]
async fn main() {
    set_tracing().unwrap();
    let mut rng = thread_rng();
    let p = PathBuf::from("db.sqlite");
    let mut db = TableMapDb::new(p);
    let mut keys = vec![];
    let keys_cnt = 400;
    let no_items = 1000;
    for _ in 0..keys_cnt {
        keys.push(format!("C/{}", generate_random_str(5)));
    }
    for _ in 0..no_items {
        let pr = generate_random_str(5);
        if let Err(e) = db.next_row(&pr) {
            error!(e);
            continue;
        }
        let mut cols = vec![];
        for _ in 0..keys_cnt {
            let ky = rng.gen_range(0..keys_cnt);
            if cols.contains(&ky) {
                continue;
            }
            cols.push(ky.clone());
            let vl = generate_random_str(10);
            db.insert(&keys[ky], &vl).unwrap()
        }
    }
    info!("{}", db.how_many_items().unwrap());
    let instant = Instant::now();
    dump_db(&mut db, Path::new("another_db.sqlite"), 100, vec![]).await.unwrap();
    info!("sqlite: {}", instant.elapsed().as_secs());

    let instant = Instant::now();
    dump_csv(&mut db, Path::new("another_db.csv"), 100, vec![]).await.unwrap();
    info!("csv: {}", instant.elapsed().as_secs());
}