use serde::{Deserialize, Serialize};
use serde_json as J;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
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

#[derive(Serialize, Deserialize)]
struct KeyValue {
    key: String,
    value: J::Value,
}

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
            let mut db: HashMap<String, J::Value> =
                J::de::from_reader(BufReader::new(snapshot)).unwrap();

            // read logs
            let log_reader = BufReader::new(File::open(log_file(&directory, version)).unwrap());
            let mut log_length = 0;
            for line in log_reader.lines() {
                log_length += 1;
                let kv: KeyValue = J::de::from_str(&line.unwrap()).unwrap();
                db.insert(kv.key, kv.value);
            }

            DbOptions {
                db: RwLock::new(db),
                db_disk: Mutex::new(DbDisk {
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

    pub fn get(&self, key: &str) -> Option<J::Value> {
        self.db.read().unwrap().get(key).map(|v| v.clone())
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
            writer
                .write(J::ser::to_string(&*hashmap).unwrap().as_ref())
                .unwrap();

            // new log file
            let log = File::create(log_file(&db_disk.directory, new_version)).unwrap();

            // update version file
            let new_file_name = temp_version_file(&db_disk.directory);
            let mut new_file = File::create(&new_file_name).unwrap();
            new_file
                .write_all(J::ser::to_string(&new_version).unwrap().as_ref())
                .unwrap();
            fs::rename(&new_file_name, &version_file(&db_disk.directory)).unwrap();

            // update RAM
            db_disk.log = BufWriter::new(log);
            db_disk.version = new_version;
            db_disk.log_length = 0;
        }

        let kv = KeyValue {
            key: key.to_string(),
            value: value,
        };
        let log_line = J::ser::to_string(&kv).unwrap();
        db_disk.log.write(log_line.as_ref()).unwrap();
        db_disk.log.write("\n".as_ref()).unwrap();
        db_disk.log.flush().unwrap();
        let mut db = self.db.write().unwrap();
        // TODO think about String / &str
        db.insert(kv.key, kv.value);
    }
}

fn snapshot_file(directory: &Path, version: u64) -> Box<Path> {
    let mut path = PathBuf::new();
    path.push(directory);
    path.push(format!("snapshot.{}", version));
    path.into_boxed_path()
}

fn log_file(directory: &Path, version: u64) -> Box<Path> {
    let mut path = PathBuf::new();
    path.push(directory);
    path.push(format!("log.{}", version));
    path.into_boxed_path()
}

fn version_file(directory: &Path) -> Box<Path> {
    let mut path = PathBuf::new();
    path.push(directory);
    path.push("version");
    path.into_boxed_path()
}
fn temp_version_file(directory: &Path) -> Box<Path> {
    let mut path = PathBuf::new();
    path.push(directory);
    path.push("version.tmp");
    path.into_boxed_path()
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use std::env::temp_dir;

    fn init() -> DbOptions {
        let path = temp_dir().into_boxed_path();
        DbOptions::init(path)
    }

    #[test]
    fn can_upsert_and_get_back() {
        let mut db = init();
        let k = "Key";
        let v = json!("Value");
        db.upsert(k, v.clone());
        let v_act = db.get(&k);
        assert_eq!(v_act, Some(v));
    }

    #[test]
    fn returns_none_when_not_inserted() {
        let db = init();
        let k = "Key";
        let v = db.get(&k);
        assert_eq!(v, None);
    }

    #[test]
    fn upsert_overwrites_when_repeated() {
        let mut db = init();
        let k = "Key";
        let v = json!("Value");
        let v2 = json!("Other");
        db.upsert(k, v.clone());
        db.upsert(k, v2.clone());
        let v_act = db.get(&k);
        assert_eq!(v_act, Some(v2));
    }

    #[test]
    fn inits_from_file() {
        let path = temp_dir().into_boxed_path();
        let k = "Key";
        let v = json!("Value");
        let k2 = "Other Key";
        let v2 = json!("Other Value");

        {
            let mut db1 = DbOptions::init(path.clone());
            db1.upsert(k, v.clone());
            db1.upsert(k2, v2.clone());
        }

        let mut db2 = DbOptions::init(path.clone());
        let v_act = db2.get(&k);
        let v2_act = db2.get(&k2);
        assert_eq!(v_act, Some(v));
        assert_eq!(v2_act, Some(v2));
    }
}
