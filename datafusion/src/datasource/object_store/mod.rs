// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Object Store abstracts access to an underlying file/object storage.

#[cfg(feature = "hdfs")]
#[allow(unused_variables)]
#[allow(unused_parens)]
pub mod hdfs;
pub mod local;

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::Read;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{AsyncRead, Stream, StreamExt};

#[cfg(feature = "hdfs")]
use crate::datasource::object_store::hdfs::HadoopFileSystem;
use local::LocalFileSystem;

use crate::error::{DataFusionError, Result};

/// Object Reader for one file in an object store.
///
/// Note that the dynamic dispatch on the reader might
/// have some performance impacts.
#[async_trait]
pub trait ObjectReader: Send + Sync {
    /// Get reader for a part [start, start + length] in the file asynchronously
    async fn chunk_reader(&self, start: u64, length: usize)
        -> Result<Box<dyn AsyncRead>>;

    /// Get reader for a part [start, start + length] in the file
    fn sync_chunk_reader(
        &self,
        start: u64,
        length: usize,
    ) -> Result<Box<dyn Read + Send + Sync>>;

    /// Get reader for the entire file
    fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
        self.sync_chunk_reader(0, self.length() as usize)
    }

    /// Get the size of the file
    fn length(&self) -> u64;
}

/// Represents a specific file or a prefix (folder) that may
/// require further resolution
#[derive(Debug)]
pub enum ListEntry {
    /// Specific file with metadata
    FileMeta(FileMeta),
    /// Prefix to be further resolved during partition discovery
    Prefix(String),
}

/// The path and size of the file.
#[derive(Debug, Clone, PartialEq)]
pub struct SizedFile {
    /// Path of the file. It is relative to the current object
    /// store (it does not specify the `xx://` scheme).
    pub path: String,
    /// File size in total
    pub size: u64,
}

/// Description of a file as returned by the listing command of a
/// given object store. The resulting path is relative to the
/// object store that generated it.
#[derive(Debug, Clone, PartialEq)]
pub struct FileMeta {
    /// The path and size of the file.
    pub sized_file: SizedFile,
    /// The last modification time of the file according to the
    /// object store metadata. This information might be used by
    /// catalog systems like Delta Lake for time travel (see
    /// https://github.com/delta-io/delta/issues/192)
    pub last_modified: Option<DateTime<Utc>>,
}

impl FileMeta {
    /// The path that describes this file. It is relative to the
    /// associated object store.
    pub fn path(&self) -> &str {
        &self.sized_file.path
    }

    /// The size of the file.
    pub fn size(&self) -> u64 {
        self.sized_file.size
    }
}

impl std::fmt::Display for FileMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} (size: {})", self.path(), self.size())
    }
}

/// Stream of files listed from object store
pub type FileMetaStream =
    Pin<Box<dyn Stream<Item = Result<FileMeta>> + Send + Sync + 'static>>;

/// Stream of list entries obtained from object store
pub type ListEntryStream =
    Pin<Box<dyn Stream<Item = Result<ListEntry>> + Send + Sync + 'static>>;

/// Stream readers opened on a given object store
pub type ObjectReaderStream =
    Pin<Box<dyn Stream<Item = Result<Arc<dyn ObjectReader>>> + Send + Sync + 'static>>;

/// A ObjectStore abstracts access to an underlying file/object storage.
/// It maps strings (e.g. URLs, filesystem paths, etc) to sources of bytes
#[async_trait]
pub trait ObjectStore: Sync + Send + Debug {
    /// Return scheme of the object store
    /// For HDFS, it will be like hdfs://namenode:port
    /// For local file system, it will be like file
    fn get_scheme(&self) -> &str;

    /// Path relative to the current object store
    /// (it does not specify the `xx://` scheme).
    fn get_relative_path<'a>(&self, uri: &'a str) -> &'a str;

    /// Returns all the files in path `prefix`
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream>;

    /// Calls `list_file` with a suffix filter
    async fn list_file_with_suffix(
        &self,
        prefix: &str,
        suffix: &str,
    ) -> Result<FileMetaStream> {
        let file_stream = self.list_file(prefix).await?;
        let suffix = suffix.to_owned();
        Ok(Box::pin(file_stream.filter(move |fr| {
            let has_suffix = match fr {
                Ok(f) => f.path().ends_with(&suffix),
                Err(_) => true,
            };
            async move { has_suffix }
        })))
    }

    /// Returns all the files in `prefix` if the `prefix` is already a leaf dir,
    /// or all paths between the `prefix` and the first occurrence of the `delimiter` if it is provided.
    async fn list_dir(
        &self,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Result<ListEntryStream>;

    /// Get object reader for one file
    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>>;
}

static LOCAL_SCHEME: &str = "file";

/// A Registry holds all the object stores at runtime with a scheme for each store.
/// This allows the user to extend DataFusion with different storage systems such as S3 or HDFS
/// and query data inside these systems.
pub struct ObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    pub object_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl fmt::Debug for ObjectStoreRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreRegistry")
            .field(
                "schemes",
                &self
                    .object_stores
                    .read()
                    .unwrap()
                    .keys()
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ObjectStoreRegistry {
    /// Create the registry that object stores can registered into.
    /// ['LocalFileSystem'] store is registered in by default to support read local files natively.
    pub fn new() -> Self {
        let mut map: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
        map.insert(LOCAL_SCHEME.to_string(), Arc::new(LocalFileSystem));

        Self {
            object_stores: RwLock::new(map),
        }
    }

    /// Adds a new store to this registry.
    /// If a store of the same prefix existed before, it is replaced in the registry and returned.
    pub fn register_store(
        &self,
        scheme: String,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let mut stores = self.object_stores.write().unwrap();
        stores.insert(scheme, store)
    }

    /// Get the store registered for scheme
    pub fn get(&self, scheme: &str) -> Option<Arc<dyn ObjectStore>> {
        let stores = self.object_stores.read().unwrap();
        stores.get(scheme).cloned()
    }

    /// Get a suitable store for the URI based on it's scheme. For example:
    /// - URI with scheme `file://` or no schema will return the default LocalFS store
    /// - URI with scheme `s3://` will return the S3 store if it's registered
    /// Returns a tuple with the store and the path of the file in that store
    /// (URI=scheme://path).
    pub fn get_by_uri<'a>(
        &self,
        uri: &'a str,
    ) -> Result<(Arc<dyn ObjectStore>, &'a str)> {
        if let Ok((store, path_relative)) = self.get_by_uri_coarse(uri) {
            Ok((store, path_relative))
        } else {
            self.get_by_uri_self_registration(uri)
        }
    }

    /// A coarse check of whether the object store is registered
    fn get_by_uri_coarse<'a>(
        &self,
        uri: &'a str,
    ) -> Result<(Arc<dyn ObjectStore>, &'a str)> {
        if let Some((scheme, _path)) = uri.split_once("://") {
            let stores = self.object_stores.read().unwrap();
            let store = stores
                .get(&*scheme.to_lowercase())
                .map(Clone::clone)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "No suitable object store found for {}",
                        scheme
                    ))
                })?;
            let path_relative = store.get_relative_path(uri);
            Ok((store, path_relative))
        } else {
            Ok((Arc::new(LocalFileSystem), uri))
        }
    }

    /// try to get an object store for the uri, like hdfs, s3
    /// If it's able to get an object store, then register it if not stored
    fn get_by_uri_self_registration<'a>(
        &self,
        uri: &'a str,
    ) -> Result<(Arc<dyn ObjectStore>, &'a str)> {
        #[cfg(feature = "hdfs")]
        {
            if let Ok((store, path_relative)) = self.get_by_hdfs_uri(uri) {
                return Ok((store, path_relative));
            }
        }

        Err(DataFusionError::Internal(format!(
            "No suitable object store found for {}",
            uri
        )))
    }

    #[cfg(feature = "hdfs")]
    /// try to get HadoopFileSystem for the uri
    fn get_by_hdfs_uri<'a>(
        &self,
        uri: &'a str,
    ) -> Result<(Arc<dyn ObjectStore>, &'a str)> {
        let mut stores = self.object_stores.write().unwrap();
        if let Ok(hdfs_store) = HadoopFileSystem::new_with_uri(uri) {
            let store = Arc::new(hdfs_store);
            let hdfs_url = store.get_scheme().to_lowercase();
            if stores.get(&hdfs_url).is_none() {
                stores.insert(hdfs_url, store.clone());
            }
            let path_relative = store.get_relative_path(uri);
            Ok((store, path_relative))
        } else {
            Err(DataFusionError::Internal(format!(
                "No suitable HadoopFileSystem found for {}",
                uri
            )))
        }
    }
}
