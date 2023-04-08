use std::collections::BTreeMap;
use std::io::{self, ErrorKind, Seek, SeekFrom};

use hashbrown::HashMap;

use crate::fragments::{
    BlockId,
    BlockInfo,
    FragmentReader,
    FragmentWriter,
    IndexFragmentsReaders,
    IndexFragmentsWriters,
};
use crate::Metastore;
/// Loads all sealed fragments stored within the metastore.
pub async fn load_readers(metastore: &Metastore) -> io::Result<IndexFragmentsReaders> {
    let fragment_ids = metastore
        .get_sealed_fragments()
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    let mut readers = BTreeMap::new();
    for fragment_id in fragment_ids {
        // We do this same process in the IndexFragmentsReaders::load_reader method
        // but it is very heavy on locking and is slower, so we use this method to
        // prevent thousands of fragments slowing the startup time.
        let path = crate::resolvers::get_fragment_location(fragment_id);
        let reader = FragmentReader::open_mmap(path).await?;
        readers.insert(fragment_id, reader);
    }

    Ok(IndexFragmentsReaders::from_existing_state(readers))
}

/// Loads / recovers partially written fragment writers.
///
/// This is a blocking operation.
///
pub async fn load_partial_writers(
    metastore: &Metastore,
) -> io::Result<IndexFragmentsWriters> {
    let metastore = metastore.clone();
    lnx_executor::spawn_task(async move { load_partial_writers_inner(&metastore) })
        .await
        .expect("Join task")
}

fn load_partial_writers_inner(
    metastore: &Metastore,
) -> io::Result<IndexFragmentsWriters> {
    let fragment_ids = metastore
        .get_unsealed_fragments()
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    let unsealed_blocks = metastore
        .get_blocks()
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
    let mut fragment_blocks = HashMap::<u64, Vec<(BlockId, BlockInfo)>>::new();
    for (block_id, metadata) in unsealed_blocks {
        let fragment_id = metadata.fragment_id;
        let info = BlockInfo {
            location: metadata.start..metadata.end,
            checksum: metadata.checksum,
        };

        fragment_blocks
            .entry(fragment_id)
            .and_modify(|blocks| blocks.push((block_id, info.clone())))
            .or_insert_with(|| vec![(block_id, info.clone())]);
    }

    let mut writers = HashMap::new();
    for fragment_id in fragment_ids {
        let path = crate::resolvers::get_fragment_location(fragment_id);

        let res = std::fs::OpenOptions::new()
            .create(false)
            .write(true)
            .read(true)
            .open(path);

        match res {
            Ok(mut file) => {
                file.seek(SeekFrom::End(0))?;

                let block_locations =
                    fragment_blocks.remove(&fragment_id).unwrap_or_default();

                let writer = FragmentWriter::from_existing_state(
                    fragment_id,
                    file,
                    metastore.clone(),
                    block_locations,
                );

                writers.insert(fragment_id, writer);
            },
            Err(e) if e.kind() == ErrorKind::NotFound => {},
            Err(e) => return Err(e),
        }
    }

    Ok(IndexFragmentsWriters::from_existing_state(
        metastore.clone(),
        writers,
    ))
}
