use indexmap::IndexMap;
use rusqlite::config::DbConfig;
use rusqlite::{params_from_iter, Connection, OpenFlags};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::slice::Chunks;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tracing::{error, info, trace, warn};
use crate::errors::DataToolErrors;

pub mod errors;

const KEY_TABLE: &str = r#"
PRAGMA temp_store = MEMORY; PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF;

BEGIN;
create table if not exists item_data
(
    id       integer not null
        constraint key_val_pk
            primary key autoincrement,
    item_val TEXT
        constraint item_data_pk
            unique
);

create table if not exists data_columns
(
    id      integer not null
        constraint key_val_pk
            primary key autoincrement,
    key     text,
    value   text,
    item_id int
        constraint data_columns_item_data_id_fk
            references item_data
            on update cascade on delete cascade
);

delete from item_data;
COMMIT;
"#;

#[derive(Debug)]
struct ColumnDef(String);

#[derive(Debug)]
pub struct ItemData {
    id: i64,
    item_val: String,
}

/// Used as temporary key value storage.
/// `item_data` -> Stores item (id, item_val)
/// `data_columns` -> Data belonging to item, stored as key, value
///
/// ## Caution
/// As the settings are for performance instead of consistency,
/// it has a high probability of getting corrupted if the program closes unexpectedly,
/// and the db file will be deleted, if so.
/// So, This must not be used for persistent storage.
///
pub struct TableMapDb {
    db_file: PathBuf,
    pub connection: Connection,
    columns: HashSet<String>,
    current_id: Option<i64>,
    current_row_iter: Option<Vec<i64>>,
}

impl TableMapDb {
    /// Tries to open the database file, the setting being used might corrupt the database
    /// so remove the file, IF the database seems corrupt. This will also create the required
    /// tables if they do not exist.
    /// If the tables exist, it will clear the data
    pub fn new(db_file: PathBuf) -> Self {
        if db_file.exists() {
            warn!("Removing db file: {:?}", db_file);
            fs::remove_file(&db_file).unwrap();
        }
        let mut connection = Connection::open(&db_file).unwrap();
        if let Err(e) = connection.execute_batch(KEY_TABLE) {
            panic!("{:?} {}", db_file, e);
        }
        info!("all good, db is ready");
        Self {
            db_file,
            connection,
            columns: Default::default(),
            current_id: None,
            current_row_iter: None,
        }
    }

    /// count the total number of items in the `item_data` table
    pub fn how_many_items(&mut self) -> Result<usize, DataToolErrors> {
        let mut stmt = self
            .connection
            .prepare_cached("select count(item_val) from item_data")
            .unwrap();
        stmt.query_row([], |r| Ok(r.get(0)?))
            .map_err(|e| DataToolErrors::GenericError(e.to_string()))
    }

    pub fn db_file(&self) -> PathBuf {
        self.db_file.clone()
    }

    pub fn read_only_conn(&self) -> Connection {
        Connection::open_with_flags(&self.db_file, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
    }

    pub fn item_ids(&self) -> Vec<i64> {
        let mut stmt = self
            .connection
            .prepare_cached("select id from item_data")
            .unwrap();
        stmt.query_map([], |r| Ok(r.get(0)?))
            .unwrap()
            .map(|v| v.unwrap())
            .collect()
    }

    pub fn next_row(&mut self, d: &str) -> rusqlite::Result<()> {
        if let Err(_) = self
            .connection
            .execute("insert into item_data (item_val) values(?1)", [d])
        {
            // maybe it exists in the db already, find it
            let mut stmt = self
                .connection
                .prepare_cached("select id from item_data where item_val = ?1")
                .unwrap();
            self.current_id = match stmt.query_row([d], |row| row.get(0)) {
                Ok(v) => Some(v),
                Err(e) => {
                    error!("Failed to get next row");
                    return Err(e);
                }
            };
        } else {
            self.current_id = Some(self.connection.last_insert_rowid());
        }
        Ok(())
    }

    pub fn insert_batched(
        &mut self,
        index_map: &IndexMap<String, String>,
    ) -> Result<(), DataToolErrors> {
        if self.current_id.is_none() {
            return Err(DataToolErrors::GenericError("No Item is set".to_string()));
        }
        let mut stmt = match self
            .connection
            .prepare_cached("insert into data_columns (key, value, item_id) values(?1, ?2, ?3)")
        {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create stmt: {}", e);
                return Err(DataToolErrors::GenericError(
                    "Failed to create statement".to_string(),
                ));
            }
        };
        index_map.iter().for_each(|(k, v)| {
            if let Err(e) = stmt.execute([
                k.clone(),
                v.clone(),
                self.current_id.clone().unwrap().to_string(),
            ]) {
                error!("Error occurred: {}", e)
            }
        });
        Ok(())
    }

    pub fn insert(&mut self, column: &str, val: &str) -> Result<(), String> {
        if self.current_id.is_none() {
            return Err("No item is set".to_string());
        }
        self.connection
            .execute(
                "insert into data_columns (key, value, item_id) values(?1, ?2, ?3)",
                [column, val, &self.current_id.clone().unwrap().to_string()],
            )
            .map_err(|v| v.to_string())?;
        Ok(())
    }

    pub fn get_distinct_keys(
        &mut self,
        mut priority_cols: Vec<String>,
    ) -> Result<Vec<String>, DataToolErrors> {
        let mut stmt = self
            .connection
            .prepare_cached("select distinct key from data_columns")
            .unwrap();
        let x: Vec<_> = stmt
            .query_map([], |row| Ok(ColumnDef(row.get(0)?)))
            .map_err(|v| DataToolErrors::GenericError(v.to_string()))?
            .filter_map(|v| {
                let k = v.unwrap().0;
                if priority_cols.contains(&k) {
                    return None;
                }
                Some(k)
            })
            .collect();
        priority_cols.extend(x);
        Ok(priority_cols)
    }
}

pub struct KeyValPair {
    key: String,
    value: String,
}

impl Iterator for TableMapDb {
    type Item = IndexMap<String, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row_iter.is_none() {
            let mut stmt = self
                .connection
                .prepare_cached("select id, item_val from item_data order by id desc")
                .unwrap();
            let nn: Vec<i64> = stmt
                .query_map([], |r| Ok(r.get(0)?))
                .unwrap()
                .map(|v| v.unwrap())
                .collect();
            self.current_row_iter = Some(nn);
        }

        if let Some(n) = self.current_row_iter.as_mut().unwrap().pop() {
            let mut inner_stmt = self
                .connection
                .prepare_cached("select key, value from data_columns where item_id = ?1")
                .unwrap();
            let rows = inner_stmt
                .query_map([n], |r| {
                    Ok(KeyValPair {
                        key: r.get(0).unwrap(),
                        value: r.get(1).unwrap(),
                    })
                })
                .unwrap();
            let mut im = IndexMap::new();
            im.insert("id".to_string(), n.to_string());
            for row in rows {
                let r = row.unwrap();
                im.insert(r.key.clone(), r.value.clone());
            }
            return Some(im);
        }
        None
    }
}

pub async fn dump_csv(
    mut db: &mut TableMapDb,
    file_name: &Path,
    chunk_size: usize,
    column_order: Vec<String>,
) -> Result<(), DataToolErrors> {
    if file_name.exists() {
        warn!("Deleting file: {:?}", file_name);
        fs::remove_file(file_name).unwrap();
    }
    let mut csv_writer = csv::Writer::from_path(&file_name)?;
    let columns = db.get_distinct_keys(column_order)?;
    let all_ids = db.item_ids();
    let ids_count = all_ids.chunks(chunk_size);
    // creating def for creating table
    csv_writer.write_record(&columns).unwrap();
    // creating def for data insertion
    let dbf = db.db_file();
    let nn = ids_count.len();
    let mut cols = proc_ids(dbf, ids_count, nn, columns.clone());
    info!("processing done");
    while let Some(c) = cols.join_next().await {
        let n = c.unwrap();
        for row in n.iter() {
            if let Err(e) = csv_writer.write_record(row) {
                error!("Failed to store data: {}", e);
            }
        }
    }
    info!("Done!");
    Ok(())
}

/// export the data in a CSV file.
pub async fn dump_db(
    mut tmd: &mut TableMapDb,
    file_name: &Path,
    chunk_size: usize,
    priority_cols: Vec<String>,
) -> Result<(), DataToolErrors> {
    if file_name.exists() {
        warn!("Deleting file: {:?}", file_name);
        fs::remove_file(file_name).unwrap();
    }
    let db = Connection::open(file_name).unwrap();
    let mut columns: Vec<_> = tmd.get_distinct_keys(priority_cols).unwrap();
    let q = format!(
        "create table products ({})",
        columns
            .iter()
            .map(|v| format!("\"{}\" TEXT", v))
            .collect::<Vec<_>>()
            .join(",")
    );
    db.execute(&q, []).unwrap();
    let pos_vals = (0..columns.len())
        .map(|v| format!("?{}", v + 1))
        .collect::<Vec<String>>()
        .join(",");
    let q = format!(
        "insert into products ({}) values ({})",
        columns
            .iter()
            .map(|v| format!("\"{}\"", v))
            .collect::<Vec<_>>()
            .join(","),
        pos_vals
    );
    let mut stmt = db.prepare_cached(&q).unwrap();
    let all_ids = tmd.item_ids();
    let ids_count = all_ids.chunks(chunk_size);
    let dbf = tmd.db_file();
    let nn = ids_count.len();
    let mut cols = proc_ids(dbf, ids_count, nn, columns.clone());
    while let Some(c) = cols.join_next().await {
        let n = c.unwrap();
        for row in n.iter() {
            if let Err(e) = stmt.execute(params_from_iter(row.iter())) {
                error!("Failed to store to db: {}", e);
            }
        }
    }
    info!("Done!");
    Ok(())
}

fn proc_ids(
    dbf: PathBuf,
    ids_count: Chunks<i64>,
    nn: usize,
    columns: Vec<String>,
) -> JoinSet<Vec<Vec<String>>> {
    let mut cols = JoinSet::new();
    for (ii, ids) in ids_count.enumerate() {
        trace!("processing ... {} of {}", ii + 1, nn);
        cols.spawn(read_db_chunked(
            dbf.clone(),
            columns.clone(),
            ids.to_vec(),
            ii,
        ));
    }
    cols
}

async fn read_db_chunked(
    file_name: PathBuf,
    columns: Vec<String>,
    ids: Vec<i64>,
    cc: usize,
) -> Vec<Vec<String>> {
    let conn = match Connection::open_with_flags(&file_name, OpenFlags::SQLITE_OPEN_READ_ONLY) {
        Ok(c) => c,
        Err(e) => {
            error!("{}", e);
            return vec![];
        }
    };
    let mut res_vec = vec![];
    let t = Instant::now();
    let ids_s: Vec<_> = ids.iter().map(|v| v.to_string()).collect();
    let mut inner_stmt = conn
        .prepare(&format!(
            "select item_id, key, value from data_columns where item_id in({}) order by item_id",
            ids_s.join(",")
        ))
        .unwrap();
    let mut im_dd: IndexMap<i64, IndexMap<String, String>> = IndexMap::new();
    let _: Vec<_> = inner_stmt
        .query_map([], |row| {
            let item_id: i64 = row.get(0)?;
            let key: String = row.get(1)?;
            let val: String = row.get(2)?;
            im_dd
                .entry(item_id)
                .and_modify(|mut v| {
                    v.insert(key.clone(), val.clone());
                })
                .or_insert_with(|| {
                    let mut im = IndexMap::new();
                    im.insert(key, val);
                    im
                });
            Ok(())
        })
        .unwrap()
        .collect();
    for (_ii, im) in im_dd.iter() {
        let prep_cols = columns
            .iter()
            .map(|k| im.get(k).cloned().unwrap_or_default())
            .collect();
        res_vec.push(prep_cols);
    }
    trace!("done processing: {}, {:2}", cc, t.elapsed().as_secs_f32());
    res_vec
}
