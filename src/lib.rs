#![feature(test)]
use futures_lite::io::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder};
use serde::{Deserialize, Serialize};
use serde_json as J;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

pub enum Error {
    InvalidPath,
}

struct DbDisk {
    directory: Box<Path>,
    log: DmaStreamWriter,
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
    pub async fn init(directory: Box<Path>) -> DbOptions {
        // exists and is directory
        if !(directory.exists() && directory.is_dir()) {
            panic!("Invalid path");
        }
        // are there already logs/snapshots

        let pointer = version_file(&directory);
        if pointer.exists() {
            let version = parse_version(&pointer);

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
                    // TODO append
                    log: DmaStreamWriterBuilder::new(
                        DmaFile::create(log_file(&directory, version))
                            .await
                            .unwrap(),
                    )
                    .build(),
                    directory: directory,
                    version: version,
                    log_length: log_length,
                }),
            }
        } else {
            let version = 0;
            let mut index = File::create(pointer).unwrap();
            index.write_all(format!("{}", version).as_ref()).unwrap();

            let empty = HashMap::new();
            let mut writer =
                BufWriter::new(File::create(snapshot_file(&directory, version)).unwrap());
            writer
                .write(J::ser::to_string(&empty).unwrap().as_ref())
                .unwrap();
            writer.flush().unwrap();
            DbOptions {
                db: RwLock::new(empty),
                db_disk: Mutex::new(DbDisk {
                    log: DmaStreamWriterBuilder::new(
                        DmaFile::create(log_file(&directory, version))
                            .await
                            .unwrap(),
                    )
                    .build(),
                    directory: directory,
                    version: version,
                    log_length: 0,
                }),
            }
        }
    }

    pub async fn close(&mut self) {
        let mut db_disk = self.db_disk.lock().unwrap();
        db_disk.log.close().await.unwrap();
    }

    pub fn get(&self, key: &str) -> Option<J::Value> {
        self.db.read().unwrap().get(key).map(|v| v.clone())
    }

    pub async fn upsert(&mut self, key: &str, value: J::Value) -> () {
        let mut db_disk = self.db_disk.lock().unwrap();
        if db_disk.log_length >= MAX_LOG_LENGTH {
            let new_version = db_disk.version + 1;
            // snapshot file
            // TODO error if file exists?
            let snapshot_file_name = snapshot_file(&db_disk.directory, new_version);
            let r_f = File::create(&snapshot_file_name);
            let f = match r_f {
                Err(_) => panic!("could not create snapshot file: {:?}", &snapshot_file_name),
                Ok(f) => f,
            };
            let mut writer = BufWriter::new(f);
            let hashmap = self.db.read().unwrap();
            writer
                .write(J::ser::to_string(&*hashmap).unwrap().as_ref())
                .unwrap();

            db_disk.log.sync().await.unwrap();
            db_disk.log.close().await.unwrap();

            // new log file
            // let log = File::create(log_file(&db_disk.directory, new_version)).unwrap();
            // TODO delete old log file (after updating version file?)
            let log = DmaStreamWriterBuilder::new(
                DmaFile::create(log_file(&db_disk.directory, new_version))
                    .await
                    .unwrap(),
            )
            .build();

            // update version file
            let new_file_name = temp_version_file(&db_disk.directory);
            let r_new_file = File::create(&new_file_name);
            let mut new_file = match r_new_file {
                Err(_) => panic!("could not create version file: {:?}", &new_file_name),
                Ok(f) => f,
            };
            new_file
                .write_all(J::ser::to_string(&new_version).unwrap().as_ref())
                .unwrap();
            fs::rename(&new_file_name, &version_file(&db_disk.directory)).unwrap();

            // update RAM
            db_disk.log = log;
            db_disk.version = new_version;
            db_disk.log_length = 0;
        }

        let kv = KeyValue {
            key: key.to_string(),
            value: value,
        };
        let log_line = J::ser::to_string(&kv).unwrap();
        db_disk.log.write(log_line.as_ref()).await.unwrap();
        db_disk.log.write("\n".as_ref()).await.unwrap();
        db_disk.log.sync().await.unwrap();
        db_disk.log_length += 1;
        let mut db = self.db.write().unwrap();
        // TODO think about String / &str
        db.insert(kv.key, kv.value);
    }
}

fn parse_version(filename: &Path) -> u64 {
    let f = File::open(filename).unwrap();
    let mut reader = BufReader::new(f);
    let mut version = String::new();
    reader.read_line(&mut version).unwrap();
    version.parse::<u64>().unwrap()
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
    use glommio::LocalExecutorBuilder;
    use serde_json::json;
    use tempfile::{tempdir, TempDir};
    extern crate test;
    use test::Bencher;

    async fn new_db() -> (TempDir, DbOptions) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("").into_boxed_path();
        let db = DbOptions::init(path).await;
        (dir, db)
    }

    #[test]
    fn can_upsert_and_get_back() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (_dir, mut db) = new_db().await;
                let k = "Key";
                let v = json!("Value");
                db.upsert(k, v.clone()).await;
                let v_act = db.get(&k);
                assert_eq!(v_act, Some(v));
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn returns_none_when_not_inserted() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (_dir, db) = new_db().await;
                let k = "Key";
                let v = db.get(&k);
                assert_eq!(v, None);
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn upsert_overwrites_when_repeated() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (_dir, mut db) = new_db().await;
                let k = "Key";
                let v = json!("Value");
                let v2 = json!("Other");
                db.upsert(k, v.clone()).await;
                db.upsert(k, v2.clone()).await;
                let v_act = db.get(&k);
                assert_eq!(v_act, Some(v2));
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn inits_from_file() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let dir = tempdir().unwrap();
                let path = dir.path().to_owned().into_boxed_path();
                let k = "Key";
                let v = json!("Value");
                let k2 = "Other Key";
                let v2 = json!("Other Value");

                {
                    let mut db1 = DbOptions::init(path.clone()).await;
                    db1.upsert(k, v.clone()).await;
                    db1.upsert(k2, v2.clone()).await;
                }

                let db2 = DbOptions::init(path.clone()).await;
                let v_act = db2.get(&k);
                let v2_act = db2.get(&k2);
                assert_eq!(v_act, Some(v));
                assert_eq!(v2_act, Some(v2));
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn version_in_memory_after_100_writes() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (_dir, mut db) = new_db().await;

                let v = json!("Value");
                for k in 1..120 {
                    db.upsert(format!("{}", k).as_ref(), v.clone()).await;
                }
                let version = db.db_disk.lock().unwrap().version;
                assert_eq!(version, 1);
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn version_on_disk_after_100_writes() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (_dir, mut db) = new_db().await;

                let v = json!("Value");
                for k in 1..120 {
                    db.upsert(format!("{}", k).as_ref(), v.clone()).await;
                }
                let directory = db.db_disk.lock().unwrap().directory.clone();
                let version_on_disk = parse_version(&version_file(&directory));
                assert_eq!(version_on_disk, 1);
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn log_file_length_after_120_writes() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (_dir, mut db) = new_db().await;

                let v = json!("Value");
                for k in 1..120 {
                    db.upsert(format!("{}", k).as_ref(), v.clone()).await;
                }
                {
                    db.close().await;
                }
                let directory = db.db_disk.lock().unwrap().directory.clone();
                let version = db.db_disk.lock().unwrap().version;
                let log_reader = BufReader::new(File::open(log_file(&directory, version)).unwrap());
                assert_eq!(version, 1);
                assert_eq!(log_reader.lines().count(), 19);
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn recover_after_100_writes() {
        let thread = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let dir = tempdir().unwrap();
                let path = dir.path().to_owned().into_boxed_path();
                let v = json!("Value");
                {
                    let mut db1 = DbOptions::init(path.clone()).await;
                    for k in 1..120 {
                        db1.upsert(format!("{}", k).as_ref(), v.clone()).await;
                    }
                }
                let db2 = DbOptions::init(path.clone()).await;
                let expected = Some(v.clone());
                for k in 1..120 {
                    let actual = db2.get(format!("{}", k).as_ref());
                    assert_eq!(actual, expected);
                }
            })
            .unwrap();
        thread.join().unwrap();
    }

    #[bench]
    fn do_nothing_baseline(b: &mut Bencher) {
        b.iter(|| {
            let thread = LocalExecutorBuilder::default().spawn(|| async {}).unwrap();
            thread.join().unwrap();
        })
    }

    #[bench]
    fn single_threaded_writes(b: &mut Bencher) {
        b.iter(|| {
            let thread = LocalExecutorBuilder::default()
                .spawn(|| async {
                    let dir = tempdir().unwrap();
                    let path = dir.path().to_owned().into_boxed_path();
                    let v = json!("Value");
                    let mut db = DbOptions::init(path.clone()).await;
                    for k in 1..1000 {
                        db.upsert(format!("{}", k).as_ref(), v.clone()).await;
                    }
                    db.close().await;
                })
                .unwrap();
            thread.join().unwrap();
        })
    }

    //    #[bench]
    //    fn single_threaded_reads(b: &mut Bencher) {
    //        with_db(|mut db| {
    //            let v = json!("Value");
    //            let keys = (1..1000).map(|k| format!("{}", k)).collect::<Vec<String>>();
    //            for k in &keys {
    //                db.upsert(&k, v.clone());
    //            }
    //            b.iter(|| {
    //                for k in &keys {
    //                    let a = db.get(&k);
    //                    assert!(a.is_some())
    //                }
    //            })
    //        })
    //    }
    //
    //    #[bench]
    //    fn four_threaded_reads(b: &mut Bencher) {
    //        with_db(|mut db| {
    //            let v = json!("Value");
    //            let keys = (1..1000).map(|k| format!("{}", k)).collect::<Vec<String>>();
    //            for k in &keys {
    //                db.upsert(&k, v.clone());
    //            }
    //            let arc_db = Arc::new(db);
    //            b.iter(|| {
    //                let mut threads = vec![];
    //                let chunks = chunk(&keys, 250);
    //                for ks in chunks {
    //                    let dbb = arc_db.clone();
    //                    let t = thread::spawn(move || {
    //                        for k in ks {
    //                            let a = dbb.get(&k);
    //                            assert!(a.is_some())
    //                        }
    //                    });
    //                    threads.push(t);
    //                }
    //                for t in threads {
    //                    t.join().unwrap();
    //                }
    //            })
    //        })
    //    }
    //
    //    fn chunk<T>(v: &[T], size: usize) -> Vec<Vec<T>>
    //    where
    //        T: Clone,
    //    {
    //        v.chunks(size).map(|x| x.into()).collect()
    //    }
}
