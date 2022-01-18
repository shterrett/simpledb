use serde_json as J;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub enum Error {
    InvalidPath,
}

struct DbOptions {
    snapshot: Box<Path>,
    log: Box<Path>,
    db: HashMap<String, J::Value>,
    // add version: u64 and read it explicitly from index file
}
impl DbOptions {
    pub fn init(directory: Box<Path>) -> DbOptions {
        // exists and is directory
        if !(directory.exists() && directory.is_dir()) {
            panic!("Invalid path");
        }
        // are there already logs/snapshohts

        let pointer = directory.join("index");
        if pointer.exists() {
            let f = File::open(pointer).unwrap();
            let mut reader = BufReader::new(f);
            let mut version = String::new();
            reader.read_line(&mut version).unwrap();
            DbOptions {
                snapshot: snapshot_file(&directory, &version),
                log: log_file(&directory, &version),
                db: init_from_file(&directory, &version),
            }
        } else {
            let version = "0";
            DbOptions {
                snapshot: snapshot_file(&directory, &version),
                log: log_file(&directory, &version),
                db: HashMap::new(),
            }
        }
    }
    pub fn get(key: &str) -> Option<J::Value> {
        panic!("later")
    }
    pub fn upsert(key: &str, value: J::Value) -> () {
        panic!("later")
        // synchronous snapshotting when logfile is "too big" for now
    }
}

fn init_from_file(directory: &Path, version: &str) -> HashMap<String, J::Value> {
    panic!("later")
}

fn snapshot_file(directory: &Path, version: &str) -> Box<Path> {
    panic!("later")
}
fn log_file(directory: &Path, version: &str) -> Box<Path> {
    panic!("later")
}
