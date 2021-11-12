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

//! Ballista executor logic

use std::sync::{Arc, RwLock};

use arrow::error::Result as ArrowResult;
use ballista_core::error::BallistaError;
use ballista_core::execution_plans::ShuffleStreamReaderExec;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_core::serde::protobuf;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use hashbrown::HashMap;
use tokio::sync::mpsc::Sender;

/// Ballista executor
pub struct Executor {
    /// Directory for storing partial results
    work_dir: String,

    /// Channels for sending partial shuffle partitions to stream shuffle reader.
    /// Key is the jobId + stageId.
    /// TODO implement clean up logic
    pub channels: RwLock<HashMap<(String, usize), Vec<Sender<ArrowResult<RecordBatch>>>>>,
}

impl Executor {
    /// Create a new executor instance
    pub fn new(work_dir: &str) -> Self {
        Self {
            work_dir: work_dir.to_owned(),
            channels: RwLock::new(HashMap::new()),
        }
    }
}

impl Executor {
    /// Execute one partition of a query stage and persist the result to disk in IPC format. On
    /// success, return a RecordBatch containing metadata about the results, including path
    /// and statistics.
    pub async fn execute_shuffle_write(
        &self,
        job_id: String,
        stage_id: usize,
        part: usize,
        plan: Arc<dyn ExecutionPlan>,
        _shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Vec<protobuf::ShuffleWritePartition>, BallistaError> {
        let exec = if let Some(shuffle_writer) =
            plan.as_any().downcast_ref::<ShuffleWriterExec>()
        {
            // find all the stream shuffle readers and bind them to the context
            let stream_shuffle_readers = find_stream_shuffle_readers(plan.clone())?;
            for shuffle_reader in stream_shuffle_readers {
                let _job_id = shuffle_reader.job_id.clone();
                let _stage_id = shuffle_reader.stage_id;
                {
                    let mut _map = self.channels.write().unwrap();
                    let _senders = _map.entry((_job_id, _stage_id)).or_insert(Vec::new());
                    _senders.push(shuffle_reader.create_record_batch_channel());
                }
            }

            // recreate the shuffle writer with the correct working directory
            if !shuffle_writer.is_push_shuffle() {
                ShuffleWriterExec::try_new_pull_shuffle(
                    job_id.clone(),
                    stage_id,
                    plan.children()[0].clone(),
                    self.work_dir.clone(),
                    shuffle_writer.shuffle_output_partitioning().cloned(),
                )
            } else {
                ShuffleWriterExec::try_new(
                    job_id.clone(),
                    stage_id,
                    plan.children()[0].clone(),
                    shuffle_writer.output_loc.clone(),
                    shuffle_writer.shuffle_output_partitioning().cloned(),
                )
            }
        } else {
            Err(DataFusionError::Internal(
                "Plan passed to execute_shuffle_write is not a ShuffleWriterExec"
                    .to_string(),
            ))
        }?;

        let partitions = exec.execute_shuffle_write(part).await?;

        println!(
            "=== [{}/{}/{}] Physical plan with metrics ===\n{}\n",
            job_id,
            stage_id,
            part,
            DisplayableExecutionPlan::with_metrics(&exec)
                .indent()
                .to_string()
        );

        Ok(partitions)
    }

    pub fn work_dir(&self) -> &str {
        &self.work_dir
    }
}

/// Returns the the streaming shuffle readers in the execution plan
pub fn find_stream_shuffle_readers(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<ShuffleStreamReaderExec>, BallistaError> {
    if let Some(shuffle_reader) = plan.as_any().downcast_ref::<ShuffleStreamReaderExec>()
    {
        Ok(vec![shuffle_reader.clone()])
    } else {
        let readers = plan
            .children()
            .into_iter()
            .map(|child| find_stream_shuffle_readers(child))
            .collect::<Result<Vec<_>, BallistaError>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(readers)
    }
}
