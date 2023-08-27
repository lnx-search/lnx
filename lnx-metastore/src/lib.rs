mod types;

use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use heed;
use heed::byteorder::LE;
use heed::types::{ByteSlice, U64};
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use rkyv::de::deserializers::SharedDeserializeMap;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{AlignedVec, Archive, CheckBytes, Deserialize, Serialize};

pub use crate::types::Key;

/// A metastore that can store custom typed keys and values.
///
/// This is useful in situations where you want to make use of
/// various LMDB properties like sorting.
pub struct CustomMetastore<K, V> {
    path: Arc<PathBuf>,
    env: Env,
    db: Database<K, V>,
}

impl<K, V> Clone for CustomMetastore<K, V> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            env: self.env.clone(),
            db: self.db,
        }
    }
}

impl<K, V> CustomMetastore<K, V>
where
    K: 'static,
    V: 'static,
{
    /// Opens or creates a new metastore in a given directory.
    pub fn open(path: &Path) -> Result<Self> {
        let inner = path.join("metastore.lmdb");

        if !inner.exists() {
            std::fs::create_dir_all(&inner)?;
        }

        let env = EnvOpenOptions::new().max_dbs(5).open(&inner)?;

        let mut txn = env.write_txn()?;
        let metastore = env.create_database(&mut txn, Some("lnx_metastore__default"))?;
        drop(txn);

        Ok(Self {
            path: Arc::new(inner),
            env,
            db: metastore,
        })
    }

    /// Opens a database within the metastore.
    pub fn open_new_database<K2, V2>(
        &self,
        name: &str,
    ) -> Result<CustomMetastore<K2, V2>>
    where
        K2: 'static,
        V2: 'static,
    {
        let mut txn = self.env.write_txn()?;
        let db = self.env.create_database(&mut txn, Some(name))?;
        drop(txn);

        Ok(CustomMetastore {
            path: self.path.clone(),
            env: self.env.clone(),
            db,
        })
    }

    #[inline]
    /// Returns the DB type wrapper.
    pub fn db(&self) -> Database<K, V> {
        self.db
    }

    #[inline]
    /// Creates a new write transaction.
    pub fn write_txn(&self) -> Result<RwTxn> {
        self.env.write_txn().map_err(|e| e.into())
    }

    #[inline]
    /// Creates a new read transaction.
    pub fn read_txn(&self) -> Result<RoTxn> {
        self.env.read_txn().map_err(|e| e.into())
    }
}

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
    inner: CustomMetastore<U64<LE>, ByteSlice>,
}

impl Metastore {
    /// Opens or creates a new metastore in a given directory.
    pub fn open(path: &Path) -> Result<Self> {
        let inner = CustomMetastore::open(path)?;
        Ok(Self { inner })
    }

    /// Opens a database within the metastore.
    pub fn open_database(&self, name: &str) -> Result<Self> {
        let inner = self.inner.open_new_database(name)?;
        Ok(Self { inner })
    }

    /// Opens a database within the metastore with a custom type signature.
    pub fn open_custom_database<K, V>(&self, name: &str) -> Result<CustomMetastore<K, V>>
    where
        K: 'static,
        V: 'static,
    {
        self.inner.open_new_database(name)
    }

    /// The location of the LMDB instance.
    pub fn location(&self) -> &Path {
        &self.inner.path
    }

    /// Inserts an entry into the main metastore.
    pub fn put<K, V>(&self, k: &K, v: &V) -> Result<()>
    where
        K: Key,
        V: Serialize<AllocSerializer<1024>>,
    {
        let key = k.to_hash();
        let value = rkyv::to_bytes::<_, 1024>(v)?;

        let mut txn = self.inner.write_txn()?;
        self.inner.db.put(&mut txn, &key, value.as_ref())?;
        txn.commit()?;

        Ok(())
    }

    /// Inserts multiple entries into the main metastore.
    ///
    /// This is part of a single bulk atomic operation.
    pub fn put_many<'a, I, K: 'a, V: 'a>(&self, values: I) -> Result<()>
    where
        K: Key,
        V: Serialize<AllocSerializer<1024>>,
        I: IntoIterator<Item = (&'a K, &'a V)>,
    {
        let mut txn = self.inner.write_txn()?;

        for (k, v) in values {
            let key = k.to_hash();
            let value = rkyv::to_bytes::<_, 1024>(v)?;
            self.inner.db.put(&mut txn, &key, value.as_ref())?;
        }

        txn.commit()?;
        Ok(())
    }

    /// Gets an entry from the main metastore.
    pub fn get<K, V>(&self, k: &K) -> Result<Option<V>>
    where
        K: Key + ?Sized,
        V: Archive,
        V::Archived: 'static
            + CheckBytes<DefaultValidator<'static>>
            + Deserialize<V, SharedDeserializeMap>,
    {
        let key = k.to_hash();
        let txn = self.inner.read_txn()?;

        if let Some(slice) = self.inner.db.get(&txn, &key)? {
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
        let mut txn = self.inner.write_txn()?;
        self.inner.db.delete(&mut txn, &key)?;
        txn.commit()?;
        Ok(())
    }

    /// Deletes multiple entries from the main metastore.
    pub fn del_many<'a, I, K: 'a>(&self, keys: I) -> Result<()>
    where
        K: Key,
        I: IntoIterator<Item = &'a K>,
    {
        let mut txn = self.inner.write_txn()?;

        for k in keys {
            let key = k.to_hash();
            self.inner.db.delete(&mut txn, &key)?;
        }

        txn.commit()?;
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

        lnx_executor::spawn_blocking_task(async move { slf.put(&k, &v) }).await?
    }

    #[inline]
    /// Gets an entry from the main metastore.
    pub async fn get_async<K, V>(&self, k: K) -> Result<Option<V>>
    where
        K: Key + Send + 'static,
        V: Archive + Send + 'static,
        V::Archived: Send
            + 'static
            + CheckBytes<DefaultValidator<'static>>
            + Deserialize<V, SharedDeserializeMap>,
    {
        let slf = self.clone();

        lnx_executor::spawn_blocking_task(async move { slf.get(&k) }).await?
    }

    #[inline]
    /// Deletes an entry from the main metastore.
    pub async fn del_async<K>(&self, k: K) -> Result<()>
    where
        K: Key + Send + 'static,
    {
        let slf = self.clone();

        lnx_executor::spawn_blocking_task(async move { slf.del(&k) }).await?
    }
}
