# SQLite-based Key Value storage for data with dynamic columns

A simple library for storing data that have dynamic number of columns in a key value based storage.

Provides an efficient way to export the data as CSV file. async required. 


## How to use?

A sample is provided in `bin/main.rs`

Additionally, columns can be prioritized to be in the beginning of the row.

```rust
fn export_csv() {
    let thrds = match std::thread::available_parallelism() {
        Ok(v) => {
            info!("using threads: {}", v);
            v.get()
        }
        Err(e) => {
            warn!("Failed to get available parallelism count");
            8
        }
    };
    let chunks = how_many / thrds;
    match table_map_db::dump_csv(
        &mut db,
        pp,
        chunks,
        /// COL1, COL8, and COL4 will be at the beginning of the row
        vec![
            "COL1",
            "COL9",
            "COL4",
        ],
        /// if Column 2, or 3 of the prioritized columns contains the matching word, the row will be skipped
        vec![
            DKVCSVFilterOption::contains(2, "some"),
            DKVCSVFilterOption::contains(3, "doe"),
        ],
    )
        .await
    {
        Ok(_) => info!("Done Saving csv data"),
        Err(e) => error!("Failed to save csv data: {}", e),
    }
}


```
