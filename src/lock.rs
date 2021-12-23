use async_lock::{RwLock as AsyncRwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

/// Single acquired lock reference that's tied to a particular
/// object key. When the last outstanding LockRef for a given
/// key goes out of scope, the underlying object lock gets dropped
/// to avoid leaking memory.
#[derive(Clone, Debug)]
pub(crate) struct LockRef<T: Hash + Eq + Clone> {
    manager: LockManager<T>,
    key: T,
    obj: Option<Arc<AsyncRwLock<u8>>>,
}

impl<T: Hash + Eq + Clone> LockRef<T> {
    pub async fn read<'lock_ref>(&'lock_ref self) -> RwLockReadGuard<'lock_ref, u8> {
        let object_lock = self
            .obj
            .as_ref()
            .expect("Illegal state, the object lock should always be present!");
        object_lock.read().await
    }

    pub async fn write<'lock_ref>(&'lock_ref self) -> RwLockWriteGuard<'lock_ref, u8> {
        let object_lock = self
            .obj
            .as_ref()
            .expect("Illegal state, the object lock should always be present!");
        object_lock.write().await
    }
}

impl<T: Hash + Eq + Clone> Drop for LockRef<T> {
    fn drop(&mut self) {
        // Drop our current reference to the shared lock.
        self.obj = None;
        // Then, attempt to drop the LockRef if it's the last outstanding
        // reference.
        self.manager.try_release_ref(&self.key);
    }
}

/// Manages locks, by a given key. This is useful if
/// we need multiple threads to coordinate on some object
/// that is dynamically created, where the locks should be
/// cleaned up once the last remaining LockRef is dropped.
///
/// One use case we're using this for: Locking on all blob operations
/// so we can guarantee consistency with external storage.
///
#[derive(Clone, Debug)]
pub(crate) struct LockManager<T: Hash + Eq + Clone> {
    active_locks: Arc<RwLock<HashMap<T, Arc<AsyncRwLock<u8>>>>>,
}

impl<T: Hash + Eq + Clone> LockManager<T> {
    pub fn new() -> LockManager<T> {
        Self {
            active_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Acquire a new lock, by the designated key. Returned LockRef
    /// objects will all share the same underlying locks. Once the
    /// last reference is dropped, the lock will be removed from the
    /// underlying hashmap.
    pub fn acquire_ref(&self, key: T) -> LockRef<T> {
        let mut locks = self.active_locks.write().unwrap();
        match locks.get(&key) {
            Some(object_lock) => LockRef {
                key: key,
                obj: Some(object_lock.clone()),
                manager: self.clone(),
            },
            None => {
                let object_lock = Arc::new(AsyncRwLock::new(0));
                let lock_ref = LockRef {
                    key: key.clone(),
                    manager: self.clone(),
                    obj: Some(object_lock.clone()),
                };
                locks.insert(key, object_lock);
                lock_ref
            }
        }
    }

    /// Attempt to release a lock by key. The underlying lock will be
    /// removed from the backing hashmap if it's the last outstanding
    /// lock reference. Otherwise, the lock will be retained in the
    /// hashmap.
    pub fn try_release_ref(&self, key: &T) {
        let mut locks = self.active_locks.write().unwrap();
        if let Some((existing_key, object_lock)) = locks.remove_entry(key) {
            if let Err(living_ref) = Arc::try_unwrap(object_lock) {
                locks.insert(existing_key, living_ref);
            }
        }
    }

    /// Count how many outstanding locks there are in the hashmap.
    pub fn lock_count(&self) -> usize {
        self.active_locks.read().unwrap().len()
    }
}

#[tokio::test]
async fn test_multi_lock_acquire() {
    let manager = LockManager::new();

    {
        let lock_ref = manager.acquire_ref(10);
        assert_eq!(1, manager.lock_count());
        {
            let _read_guard = lock_ref.read().await;
        }
        let lock_ref_2 = manager.acquire_ref(10);
        let _write_guard = lock_ref_2.write().await;
        assert_eq!(1, manager.lock_count());
        let _lock_ref_3 = manager.acquire_ref(11);
        assert_eq!(2, manager.lock_count());
    }

    assert_eq!(0, manager.lock_count());
}
