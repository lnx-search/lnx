mod types;

use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use heed::byteorder::LE;
use heed::types::{ByteSlice, U64};
use heed::{Database, Env, EnvOpenOptions};
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{AlignedVec, Archive, CheckBytes, Deserialize, Serialize};

pub use crate::types::Key;

#[derive(Clone)]
/// The metadata storage system.
///
/// This is a ACID key-value store which can be used
/// in places where it's important to keep the data
/// correctly persisted.
///
/// This is a core structure and only persists data locally.
/// The `ReplicatedMetastore` should be used for adjusting settings
/// that need to be reflected across the cluster.
pub struct Metastore {
    path: Arc<PathBuf>,
    env: Env,
    db: Database<U64<LE>, ByteSlice>,
}

impl Metastore {
    /// Opens or creates a new metastore in a given directory.
    pub fn open(path: &Path) -> Result<Self> {
        let inner = path.join("metastore.lmdb");

        if !inner.exists() {
            std::fs::create_dir_all(&inner)?;
        }

        let env = EnvOpenOptions::new().max_dbs(5).open(&inner)?;

        let mut txn = env.write_txn()?;
        let metastore = env.create_database(&mut txn, Some("metastore"))?;
        drop(txn);

        Ok(Self {
            path: Arc::new(inner),
            env,
            db: metastore,
        })
    }

    /// Opens a database within the metastore.
    pub fn open_database(&self, name: &str) -> Result<Self> {
        let mut txn = self.env.write_txn()?;
        let db = self.env.create_database(&mut txn, Some(name))?;
        drop(txn);

        Ok(Self {
            path: self.path.clone(),
            env: self.env.clone(),
            db,
        })
    }

    /// The location of the LMDB instance.
    pub fn location(&self) -> &Path {
        &self.path
    }

    /// Inserts an entry into the main metastore.
    pub fn put<K, V>(&self, k: &K, v: &V) -> Result<()>
    where
        K: Key,
        V: Serialize<AllocSerializer<1024>>,
    {
        let key = k.to_hash();
        let value = rkyv::to_bytes::<_, 1024>(v)?;

        let mut txn = self.env.write_txn()?;
        self.db.put(&mut txn, &key, value.as_ref())?;
        Ok(())
    }

    /// Gets an entry from the main metastore.
    pub fn get<K, V>(&self, k: &K) -> Result<Option<V>>
    where
        K: Key,
        V: Archive,
        V::Archived: 'static
            + CheckBytes<DefaultValidator<'static>>
            + Deserialize<V, SharedDeserializeMap>,
    {
        let key = k.to_hash();
        let txn = self.env.read_txn()?;

        if let Some(slice) = self.db.get(&txn, &key)? {
            let mut bytes = AlignedVec::with_capacity(slice.len());
            bytes.extend_from_slice(slice);

            // SAFETY:
            // Technically this may look unsound, but we know that rkyv does not implement `Archive`
            // for static lifetimes, things like `&'static str` will not work and
            // `Cow<'static, str>` requires a `AsOwned` with modifier to deserialize with an
            // owned value instead.
            let slice = unsafe { mem::transmute::<&[u8], &'static [u8]>(&bytes) };
            let value = rkyv::from_bytes::<V>(slice)
                .map_err(|e| anyhow!("Cannot deserialize metastore entry: {e}"))?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Deletes an entry from the main metastore.
    pub fn del<K>(&self, k: &K) -> Result<()>
    where
        K: Key,
    {
        let key = k.to_hash();
        let mut txn = self.env.write_txn()?;
        self.db.delete(&mut txn, &key)?;
        Ok(())
    }

    #[inline]
    /// Inserts an entry into the main metastore.
    pub async fn put_async<K, V>(&self, k: K, v: V) -> Result<()>
    where
        K: Key + Send + 'static,
        V: Serialize<AllocSerializer<1024>> + Send + 'static,
    {
        let slf = self.clone();

        lnx_executor::spawn_blocking_task(async move {
            slf.put(&k, &v)
        }).await?
    }

    #[inline]
    /// Gets an entry from the main metastore.
    pub async fn get_async<K, V>(&self, k: K) -> Result<Option<V>>
    where
        K: Key + Send + 'static,
        V: Archive + Send + 'static,
        V::Archived: Send + 'static
            + CheckBytes<DefaultValidator<'static>>
            + Deserialize<V, SharedDeserializeMap>,
    {
        let slf = self.clone();

        lnx_executor::spawn_blocking_task(async move {
            slf.get(&k)
        }).await?
    }

    #[inline]
    /// Deletes an entry from the main metastore.
    pub async fn del_async<K>(&self, k: K) -> Result<()>
    where
        K: Key + Send + 'static,
    {
        let slf = self.clone();

        lnx_executor::spawn_blocking_task(async move {
            slf.del(&k)
        }).await?
    }
}
