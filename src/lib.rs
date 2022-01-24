use serde_json as J;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

pub enum Error {
    InvalidPath,
}

struct DbDisk {
    directory: Box<Path>,
    log: BufWriter<File>,
    version: u64,
    log_length: u64,
}

pub struct DbOptions {
    // always take log lock before db lock
    db: RwLock<HashMap<String, J::Value>>,
    db_disk: Mutex<DbDisk>, // add version: u64 and read it explicitly from index file
}

const MAX_LOG_LENGTH: u64 = 100;
pub type Db = Arc<DbOptions>;

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
            let db: HashMap<String, J::Value> =
                J::de::from_reader(BufReader::new(snapshot)).unwrap();

            // read logs
            let log_reader = BufReader::new(File::open(log_file(&directory, version)).unwrap());
            let mut log_length = 0;
            for line in log_reader.lines() {
                log_length += 1;
                // TODO implement
            }

            // TODO init new log file?  or append to old one?
            DbOptions {
                db: RwLock::new(db),
                db_disk: Mutex::new(DbDisk {
                    // TODO make it compile
                    log: BufWriter::new(
                        OpenOptions::new()
                            .append(true)
                            .open(log_file(&directory, version))
                            .unwrap(),
                    ),
                    directory: directory,
                    version: version,
                    log_length: log_length,
                }),
            }
        } else {
            let version = 0;
            DbOptions {
                db: RwLock::new(HashMap::new()),
                db_disk: Mutex::new(DbDisk {
                    log: BufWriter::new(File::create(log_file(&directory, version)).unwrap()),
                    directory: directory,
                    version: version,
                    log_length: 0,
                }),
            }
        }
    }

    pub fn get(key: &str) -> Option<J::Value> {
        panic!("later")
    }

    pub fn upsert(&mut self, key: &str, value: J::Value) -> () {
        let mut db_disk = self.db_disk.lock().unwrap();
        if db_disk.log_length >= MAX_LOG_LENGTH {
            let new_version = db_disk.version + 1;
            // snapshot file
            // TODO error if file exists?
            let f = File::create(snapshot_file(&db_disk.directory, new_version)).unwrap();
            let mut writer = BufWriter::new(f);
            let hashmap = self.db.read().unwrap();
            writer.write(J::ser::to_string(&*hashmap).unwrap().as_ref()).unwrap();

            // new log file
            let log = File::create(log_file(&db_disk.directory, new_version)).unwrap();

            // update version file
            let new_file_name = temp_version_file(&db_disk.directory);
            let mut new_file = File::create(&new_file_name).unwrap();
            new_file.write_all(J::ser::to_string(&new_version).unwrap().as_ref()).unwrap();
            fs::rename(&new_file_name, &version_file(&db_disk.directory)).unwrap();

            // update RAM
            db_disk.log = BufWriter::new(log);
            db_disk.version = new_version;
            db_disk.log_length = 0;
        }

        db_disk
            .log
            .write(format!("{}\x00{}\n", &key, &value).as_ref())
            .unwrap();
        db_disk.log.flush().unwrap();
        let mut db = self.db.write().unwrap();
        // TODO think about String / &str
        db.insert(key.to_string(), value);
    }
}

fn init_from_file(directory: &Path, version: u64) -> HashMap<String, J::Value> {
    // TODO did I accidentally inline this into init()? – bergey 2022-01-24
    panic!("later")
}

fn snapshot_file(directory: &Path, version: u64) -> Box<Path> {
    panic!("later")
}
fn log_file(directory: &Path, version: u64) -> Box<Path> {
    panic!("later")
}

fn version_file(directory: &Path) -> Box<Path> {
    panic!("later")
}
fn temp_version_file(directory: &Path) -> Box<Path> {
    panic!("later")
}
