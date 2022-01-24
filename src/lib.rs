use serde_json as J;
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

pub enum Error {
    InvalidPath,
}

struct DbDisk {
    directory: Box<Path>,
    log: BufWriter<File>,
    version: u64,
    log_length: u64,
}

struct DbOptions {
    // always take log lock before db lock
    db: RwLock<HashMap<String, J::Value>>,
    db_disk: Mutex<DbDisk>
    // add version: u64 and read it explicitly from index file
}

type Db = Arc<DbOptions>;

// TODO KeyValue type

impl DbOptions {
    pub fn init(directory: Box<Path>) -> DbOptions {
        // exists and is directory
        if !(directory.exists() && directory.is_dir()) {
            panic!("Invalid path");
        }
        // are there already logs/snapshots

        let pointer = directory.join("index");
        if pointer.exists() {
            let f = File::open(pointer).unwrap();
            let mut reader = BufReader::new(f);
            let mut version = String::new();
            reader.read_line(&mut version).unwrap();
            let version = version.parse::<u64>().unwrap();

            // read last snapshot
            let snapshot = File::open(snapshot_file(&directory, version)).unwrap();
            let db = J::de::from_reader(BufReader::new(snapshot)).unwrap();
            
            // read logs
            let log_file = BufReader::new(File::open(log_file(&directory, version)));
            for line in log_file.lines() {
                // TODO implement
            }
            
            // TODO init new log file?  or append to old one?
            DbOptions {
                db: RwLock::new(init_from_file(&directory, version)),
                db_disk: Mutex::new( DbDisk {
                    directory: directory,
                    log:  // TODO make it compile
                    log: BufWriter::new(File.create(log_file(&directory, &version))),
                snapshot: snapshot_file(&directory, &version),
                })
            }
        } else {
            let version = "0";
            DbOptions {
                db: RwLock::new(HashMap::new()),
                db_disk: Mutex::new( DbDisk {
                    directory: directory,
                    log: BufWriter::new(File::create(log_file(&directory, &version))),
                    version: version,
                    log_length: 0
                }),
            }
        }
    }

    pub fn get(key: &str) -> Option<J::Value> {
        panic!("later")
    }

    pub fn upsert(&mut self, key: &str, value: J::Value) -> () {
        self.db_disk.lock(|db_disk| {
            if (db_disk.log_length >= max_log_length) {
                let new_version = db_disk.version + 1;
                // snapshot file
                // TODO error if file exists?
                let f = File::create(snapshot_file(db_disk.directory, new_version)).unwrap();
                let mut writer = BufWriter::new(f);
                let hashmap = self.db.read().unwrap();
                writer.write(J::ser::to_string(hashmap));

                // new log file
                let log = File::create(log_file(db_disk.directory, new_version)).unwrap();

                // update version file
                let new_file_name = temp_version_file(db_disk.directory);
                let new_file = File::create(new_file_name).unwrap();
                new_file.write_all(new_version);
                fs::rename(new_file_name, version_file(db_disk.directory)).unwrap();

                // update RAM
                db_disk.log = Arg::new(Mutex::new(BufWriter::new(log)));
                db_disk.version = new_version;
                db_disk.log_length = 0;
            }

            db_disk.log.write(format!("{}\x00{}\n", key, value)).unwrap();
            db_disk.log.flush.unwrap();
            let mut db = self.db.write();
            db.insert(key, value);
            })
        }
    }

fn init_from_file(directory: &Path, version: u64) -> HashMap<String, J::Value> {
    panic!("later")
}

fn snapshot_file(directory: &Path, version: u64) -> Box<Path> {
    panic!("later")
}
fn log_file(directory: &Path, version: u64) -> Box<Path> {
    panic!("later")
}

fn version_file(directory: &Path, version: u64) -> Box<Path> {
    panic!("later")
}
fn temp_version_file(directory: &Path, version: u64) -> Box<Path> {
    panic!("later")
}
