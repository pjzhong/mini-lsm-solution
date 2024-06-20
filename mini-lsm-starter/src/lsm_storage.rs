#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        if let Some(flush_thread) = self.flush_thread.lock().take() {
            if let Err(e) = flush_thread.join() {
                return Err(anyhow!("join flush thread error, msg:{:?}", e));
            }
        }

        if let Some(flush_thread) = self.compaction_thread.lock().take() {
            if let Err(e) = flush_thread.join() {
                return Err(anyhow!("join flush thread error, msg:{:?}", e));
            }
        }

        if !self.inner.options.enable_wal {
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            }

            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if path.is_file() {
            return Err(anyhow!("{:?} must be a directory ", path));
        }

        fs::create_dir_all(path)?;

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let mut storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        recover(&mut storage)?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()?;
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let storage = self.state.read();

        if let Some(bytes) = storage.memtable.get(key) {
            return Ok(if bytes.is_empty() {
                None
            } else {
                Some(bytes.clone())
            });
        }

        for mem_table in &storage.imm_memtables {
            if let Some(bytes) = mem_table.get(key) {
                return Ok(if bytes.is_empty() {
                    None
                } else {
                    Some(bytes.clone())
                });
            }
        }

        let key_slice = KeySlice::from_slice(key);
        for idx in storage
            .l0_sstables
            .iter()
            .chain(storage.levels.iter().flat_map(|(_, ids)| ids))
        {
            let sst_table = match storage.sstables.get(idx) {
                Some(sst_table) if sst_table.key_may_contains(&key_slice) => sst_table.clone(),
                _ => continue,
            };

            let iter = SsTableIterator::create_and_seek_to_key(sst_table, key_slice)?;
            if !iter.is_valid() {
                continue;
            }

            if iter.key() != key_slice {
                continue;
            }

            return Ok(if iter.value().is_empty() {
                None
            } else {
                Some(Bytes::copy_from_slice(iter.value()))
            });
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for batch in batch {
            let (key, value) = match batch {
                WriteBatchRecord::Put(key, value) => (key.as_ref(), value.as_ref()),
                //Why Default::default work, but &[] doesn't????
                WriteBatchRecord::Del(key) => (key.as_ref(), Default::default()),
            };

            if key.is_empty() {
                return Err(anyhow!("Key is empty"));
            }

            let (res, approximate_size) = {
                let state = self.state.read();
                let res = state.memtable.put(key, value);
                (res, state.memtable.approximate_size())
            };
            match res {
                Ok(_) => {
                    if self.memtable_reaches_capacity_on_put(approximate_size) {
                        let lock = self.state_lock.lock();
                        if self.memtable_reaches_capacity_on_put(
                            self.state.read().memtable.approximate_size(),
                        ) {
                            return self.force_freeze_memtable(&lock);
                        }
                    }
                }
                e @ Err(_) => return e,
            }
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    fn memtable_reaches_capacity_on_put(&self, size: usize) -> bool {
        self.options.target_sst_size <= size
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(crate) fn path_of_manifest(&self) -> PathBuf {
        Self::path_of_manifest_static(&self.path)
    }

    pub(crate) fn path_of_manifest_static(path: impl AsRef<Path>) -> PathBuf {
        path.as_ref().join("MANIFEST")
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?
            .sync_all()
            .with_context(|| format!("failed to sync on dir:{:?}", self.path))
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let mut lock = self.state.write();
        //为什么要克隆，保持state的整体引用不会突然发生变化？
        //我在更新state.memtable时候会其改变指向。如果不克隆，其它线程就会发现state.memtable在使用中突然改变了。
        let mut state = lock.as_ref().clone();
        let id = self.next_sst_id();
        let mem_table = Arc::new(MemTable::create_with_wal(id, self.path_of_wal(id))?);
        let old_mem_table = std::mem::replace(&mut state.memtable, mem_table);

        state.imm_memtables.insert(0, old_mem_table);
        if let Some(manifest) = &self.manifest {
            manifest.add_record(state_lock_observer, ManifestRecord::NewMemtable(id))?;
        }

        // dereferencing RwLockWriteGuard
        // 改完之后，更新回去
        *lock = Arc::new(state);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        //state lock必须立刻获取，这时候其它线程可以正常读或部分写， 但无法对state里面的进行结构行更改，如增加新的memtable,imm_memtable,sstable等等
        //获取state的读锁，复制一个快照接着放弃
        //根据这个快照创建sst_table
        //获取写锁，等待其他读锁全部完成
        //更新state
        //完成

        let state_lock_observer = self.state_lock.lock();

        //创建快照
        let mut state = {
            let read_lock = self.state.read();
            let state = read_lock.as_ref().clone();
            state
        };

        //开始构建sst_table
        let mem_table = match state.imm_memtables.pop() {
            Some(mem_table) => mem_table,
            None => return Ok(()),
        };
        let sst_id = mem_table.id();

        let mut builder = SsTableBuilder::new(self.options.block_size);
        mem_table.flush(&mut builder)?;
        let sst_path = self.path_of_sst(sst_id);

        let sst_table = builder.build(sst_id, Some(self.block_cache.clone()), sst_path)?;
        state.sstables.insert(sst_id, Arc::new(sst_table));
        if self.compaction_controller.flush_to_l0() {
            state.l0_sstables.insert(0, sst_id);
        } else {
            state.levels.insert(0, (sst_id, vec![sst_id]))
        }

        //更新回去
        {
            let mut lock = self.state.write();
            *lock = Arc::new(state);
        }

        if let Some(manifest) = self.manifest.as_ref() {
            manifest.add_record(&state_lock_observer, ManifestRecord::Flush(sst_id))?;
            self.sync_dir()?;
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut mem_iters = vec![];
        mem_iters.push(state.memtable.scan(lower, upper).into());
        mem_iters.append(
            &mut state
                .imm_memtables
                .clone()
                .into_iter()
                .map(|m| m.scan(lower, upper).into())
                .collect(),
        );

        let key_slice = match lower {
            Bound::Included(x) => KeySlice::from_slice(x),
            Bound::Excluded(x) => KeySlice::from_slice(x),
            Bound::Unbounded => KeySlice::default(),
        };
        let mut sst_iters = vec![];
        for idx in &state.l0_sstables {
            let sst_table = match state.sstables.get(idx) {
                Some(sst_table) if sst_table.range_overlap(lower, upper) => sst_table.clone(),
                _ => continue,
            };

            let iter = {
                let mut iter = SsTableIterator::create_and_seek_to_key(sst_table, key_slice)?;

                if let Bound::Excluded(x) = lower {
                    if x == key_slice.raw_ref() {
                        iter.next()?;
                    }
                }

                if !iter.is_valid() {
                    continue;
                }
                iter
            };

            sst_iters.push(iter.into());
        }

        let concat_iter = {
            let mut concat_iters = vec![];
            for ids in state.levels.iter().map(|(_, ids)| ids) {
                let mut sstables = vec![];
                for id in ids {
                    let sst_table = match state.sstables.get(id) {
                        Some(sst_table) if sst_table.range_overlap(lower, upper) => {
                            sst_table.clone()
                        }
                        _ => continue,
                    };

                    sstables.push(sst_table);
                }

                let mut concat_iter =
                    SstConcatIterator::create_and_seek_to_key(sstables, key_slice)?;
                if let Bound::Excluded(x) = lower {
                    if x == key_slice.raw_ref() {
                        concat_iter.next()?;
                    }
                }

                let iter = if concat_iter.is_valid() {
                    concat_iter
                } else {
                    SstConcatIterator::create_and_seek_to_first(vec![])?
                };

                concat_iters.push(iter.into());
            }

            MergeIterator::create(concat_iters)
        };

        let a = TwoMergeIterator::create(
            MergeIterator::create(mem_iters),
            MergeIterator::create(sst_iters),
        )?;
        let iter = LsmIterator::new(TwoMergeIterator::create(a, concat_iter)?, upper)?;

        Ok(FusedIterator::new(iter))
    }
}

fn recover(storage: &mut LsmStorageInner) -> Result<(), anyhow::Error> {
    let (manifest, records) = Manifest::recover(storage.path_of_manifest())?;

    let mut snapshot: LsmStorageState = storage.state.read().as_ref().clone();
    let mut mem_ids = HashSet::new();
    for record in records {
        match record {
            ManifestRecord::Flush(id) => {
                let path = storage.path_of_sst(id);
                if path.is_file() {
                    snapshot.l0_sstables.insert(0, id);
                }
            }
            ManifestRecord::Compaction(task, sst_ids) => {
                let (snap_shot, _) = storage
                    .compaction_controller
                    .apply_compaction_result(&snapshot, &task, &sst_ids, true);
                snapshot = snap_shot;
            }
            ManifestRecord::NewMemtable(id) => {
                mem_ids.insert(id);
            }
        }
    }

    let mut max_sst_id = 0;
    for id in snapshot
        .l0_sstables
        .iter()
        .chain(snapshot.levels.iter().flat_map(|(_, ids)| ids.iter()))
    {
        let id = *id;
        max_sst_id = max_sst_id.max(id);
        let path = storage.path_of_sst(id);

        let sst = FileObject::open(&path)?;
        let sst = SsTable::open(id, Some(storage.block_cache.clone()), sst)?;
        snapshot.sstables.insert(sst.sst_id(), sst.into());

        mem_ids.remove(&id);
    }

    for level in &mut snapshot.levels {
        level.1.sort_by_key(|id| {
            snapshot
                .sstables
                .get(id)
                .map(|table| table.first_key())
                .with_context(|| format!("recover levels failed, sst_table not found:{id:?}"))
                .unwrap()
        });
    }

    let mem_ids = {
        let mut mem_ids = Vec::from_iter(mem_ids);
        mem_ids.sort();
        mem_ids.reverse();
        mem_ids
    };
    let next_sst_id = mem_ids.iter().max().unwrap_or(&1).max(&max_sst_id) + 1;

    {
        //根据所有 mem_id来进行恢复，然后一个新的mem_table
        let mut write_lock = storage.state.write();
        storage.next_sst_id = next_sst_id.into();
        snapshot.memtable = {
            let sst_id = storage.next_sst_id();
            if storage.options.enable_wal {
                MemTable::create_with_wal(sst_id, storage.path_of_wal(sst_id))?.into()
            } else {
                MemTable::create(sst_id).into()
            }
        };

        for mem_id in mem_ids {
            let mem_table = Arc::new(MemTable::recover_from_wal(
                mem_id,
                storage.path_of_wal(mem_id),
            )?);
            snapshot.imm_memtables.push(mem_table);
        }
        *write_lock = snapshot.into();
    }

    storage.manifest = Some(manifest);

    Ok(())
}
