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

use std::pin::Pin;

use futures::Future;

use datafusion::datasource::object_store::hdfs::HDFS_SCHEME;
use datafusion::test_util::hdfs::run_hdfs_test;

use super::*;

#[tokio::test]
async fn parquet_query() {
    run_with_register_alltypes_parquet(|mut ctx| {
        Box::pin(async move {
            // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
            // so we need an explicit cast
            let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
            let actual = execute_to_batches(&mut ctx, sql).await;
            let expected = vec![
                "+----+-----------------------------------------+",
                "| id | CAST(alltypes_plain.string_col AS Utf8) |",
                "+----+-----------------------------------------+",
                "| 4  | 0                                       |",
                "| 5  | 1                                       |",
                "| 6  | 0                                       |",
                "| 7  | 1                                       |",
                "| 2  | 0                                       |",
                "| 3  | 1                                       |",
                "| 0  | 0                                       |",
                "| 1  | 1                                       |",
                "+----+-----------------------------------------+",
            ];

            assert_batches_eq!(expected, &actual);

            Ok(())
        })
    })
    .await
    .unwrap()
}

/// Run query after table registered with parquet file on hdfs
pub async fn run_with_register_alltypes_parquet<F>(test_query: F) -> Result<()>
where
    F: FnOnce(
            ExecutionContext,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>
        + Send
        + 'static,
{
    run_hdfs_test("alltypes_plain.parquet".to_string(), |fs, filename_hdfs| {
        Box::pin(async move {
            let mut ctx = ExecutionContext::new();
            ctx.register_object_store(HDFS_SCHEME, Arc::new(fs));
            let table_name = "alltypes_plain";
            println!(
                "Register table {} with parquet file {}",
                table_name, filename_hdfs
            );
            ctx.register_parquet(table_name, &filename_hdfs).await?;

            test_query(ctx).await
        })
    })
    .await
}
