use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    str::FromStr,
    sync::Arc,
};

use ethers::{
    providers::{Http, Middleware, Provider, ProviderError},
    types::{BlockNumber, Filter, FilterBlockOption, Log, Topic, ValueOrArray, H160},
};
use tokio::sync::mpsc;

/// If an RPC fails, how many times to retry it
const MAX_RPC_RETRY_COUNT: usize = 3;

/// How many concurrent eth_getLogs requests to have at a time
const MAX_CONCURRENT_BATCHES: usize = 20;

#[tokio::main]
async fn main() {
    // Change this to whatever you want

    let config = EventDumpConfig {
        from_block: 60800000u64,
        to_block: 73939000u64,
        rpc_block_range: 2000u64,

        addresses: ValueOrArray::Array(
            vec![
                // e.g. "Main Hub" Bastion Comptroller
                "6De54724e128274520606f038591A00C5E94a1F6",
            ]
            .into_iter()
            .map(|a| H160::from_str(a).unwrap())
            .collect(),
        ),
        topics: [None, None, None, None],

        output_file_path: "/tmp/bastion-main-hub-events".into(),
        rpc_url: "https://mainnet.aurora.dev".into(),
    };

    let dump = EventDump::new(config);
    dump.fetch_and_dump().await;
}

/// Wrapper of fetched logs w/ batch ID to preserve write ordering
/// when dealing with concurrent requests racing each other
#[derive(Debug)]
struct FetchedLogs {
    // A batch is a single eth_getLogs request for a block range.
    // Each batch has an id 1 greater than the previous batch.
    // E.g. range [A, B] may have batch id 0, range [B+1, C] batch id 1, etc.
    batch_id: u32,
    logs: Vec<Log>,
}

struct EventDumpConfig {
    /// Start block of the range to get events for
    from_block: u64,
    /// End block of the range to get events for (inclusive)
    to_block: u64,
    // Max "page size" in blocks per eth_getLogs request (aka batch)
    rpc_block_range: u64,

    /// Contract addresses to get events for
    addresses: ValueOrArray<H160>,
    /// Topics to filter by
    topics: [Option<Topic>; 4],

    /// Path to file to dump to. Note this will create the file if necessary
    /// and will append contents
    output_file_path: String,

    // RPC url of the provider to use (http only atm)
    rpc_url: String,
}

struct EventDump {
    config: EventDumpConfig,
    provider: Arc<Provider<Http>>,
}

impl EventDump {
    fn new(config: EventDumpConfig) -> Self {
        let provider =
            Provider::<Http>::try_from(&config.rpc_url).expect("Couldn't create HTTP provider");
        Self {
            config,
            provider: Arc::new(provider),
        }
    }

    pub async fn fetch_and_dump(&self) {
        // This thread receives from spawned batch tasks when a batch has finished
        let (batch_id_completed_sender, mut batch_id_completed_receiver) =
            mpsc::unbounded_channel::<u32>();
        // Used for the LogWriter task to receive fetched logs from spawned batch tasks
        let (logs_sender, logs_receiver) = mpsc::unbounded_channel::<FetchedLogs>();

        // Receives fetched logs & writes them in the correct order to a file
        let log_writer = LogWriter::new(self.config.output_file_path.clone(), logs_receiver);
        let writer_handle = tokio::spawn(log_writer.wait_for_and_write_logs());

        let max_block = self.config.to_block;
        let rpc_block_range = self.config.rpc_block_range;

        let mut from_block = self.config.from_block;
        let mut to_block = std::cmp::min(from_block + rpc_block_range, max_block);

        let mut batch_id = 0;
        let mut active_batches = 0;

        while from_block <= max_block {
            // Blocks if the max # of active batches has been hit,
            // waiting for at least one to complete.
            // Reads how many batches have finished
            active_batches -= self
                .get_receive_count_from_channel(
                    &mut batch_id_completed_receiver,
                    active_batches >= MAX_CONCURRENT_BATCHES,
                )
                .await;

            // Create a new batch
            active_batches += 1;
            // Spawn a new task for the batch
            tokio::spawn(EventDump::fetch_logs_with_retries(
                self.provider.clone(),
                batch_id,
                from_block,
                to_block,
                self.config.addresses.clone(),
                self.config.topics.clone(),
                batch_id_completed_sender.clone(),
                logs_sender.clone(),
            ));
            // Bump batch_id
            batch_id += 1;

            from_block = to_block + 1;
            to_block = std::cmp::min(from_block + rpc_block_range, max_block);
        }

        // Wait for all batches to finish
        while active_batches > 0 {
            batch_id_completed_receiver.recv().await;
            active_batches -= 1;
        }

        drop(logs_sender);

        println!("Joining writer_handle...");
        writer_handle.await.expect("Couldn't join writer_handle");
        println!("Joined, all done");
    }

    // Pretty hacky - attempts to read as many messages as possible from the receiver,
    // returning how many were read. Blocks for the first read if blocking is true.
    async fn get_receive_count_from_channel<T>(
        &self,
        receiver: &mut mpsc::UnboundedReceiver<T>,
        blocking: bool,
    ) -> usize {
        let mut receive_count = 0;
        // Block for the first one
        if blocking {
            receiver.recv().await;
            receive_count += 1;
        }

        // Try to "drain" the receiver, doesn't really matter that this is inherently race-y
        loop {
            match receiver.try_recv() {
                Ok(_) => receive_count += 1,
                Err(mpsc::error::TryRecvError::Empty) => {
                    break;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    panic!("Channel disconnected")
                }
            }
        }

        receive_count
    }

    async fn fetch_logs_with_retries(
        provider: Arc<Provider<Http>>,
        batch_id: u32,
        from_block: u64,
        to_block: u64,
        addresses: ValueOrArray<H160>,
        topics: [Option<Topic>; 4],
        on_batch_id_completed: mpsc::UnboundedSender<u32>,
        fetched_logs_sender: mpsc::UnboundedSender<FetchedLogs>,
    ) -> Result<(), anyhow::Error> {
        let mut logs = vec![];

        for i in 0..MAX_RPC_RETRY_COUNT {
            match EventDump::get_logs(
                provider.clone(),
                batch_id,
                from_block,
                to_block,
                addresses.clone(),
                topics.clone(),
            )
            .await
            {
                Ok(l) => {
                    logs = l;
                    break;
                }
                Err(err) => {
                    eprintln!(
                        "Batch ID {:?} got error {:?}, rpc retry count {:?}",
                        batch_id, err, i
                    );

                    if i == MAX_RPC_RETRY_COUNT - 1 {
                        return Err(err.into());
                    }
                }
            }
        }

        on_batch_id_completed.send(batch_id)?;
        fetched_logs_sender.send(FetchedLogs { batch_id, logs })?;

        Ok(())
    }

    async fn get_logs(
        provider: Arc<Provider<Http>>,
        batch_id: u32,
        from_block: u64,
        to_block: u64,
        addresses: ValueOrArray<H160>,
        topics: [Option<Topic>; 4],
    ) -> Result<Vec<Log>, ProviderError> {
        let filter = Filter {
            block_option: FilterBlockOption::Range {
                from_block: Some(BlockNumber::Number(from_block.into())),
                to_block: Some(BlockNumber::Number(to_block.into())),
            },
            address: Some(addresses),
            topics,
        };
        println!(
            "Getting logs in block range {:?} to {:?} inclusive (batch ID {:?})...",
            from_block, to_block, batch_id
        );
        provider.get_logs(&filter).await
    }
}

struct LogWriter {
    file: File,
    logs_receiver: mpsc::UnboundedReceiver<FetchedLogs>,
}

impl LogWriter {
    fn new(path: String, logs_receiver: mpsc::UnboundedReceiver<FetchedLogs>) -> Self {
        Self {
            file: OpenOptions::new()
                .create(true) // just opens if it already exists
                .write(true)
                .append(true)
                .open(path)
                .unwrap(),
            logs_receiver,
        }
    }

    async fn wait_for_and_write_logs(mut self) {
        // We want to only write sequential batch ids
        let mut next_batch_id = 0;
        // Batches that are completed but we can't write yet because
        // we want to preserve ordering of batches
        let mut future_batches = HashMap::<u32, FetchedLogs>::new();

        loop {
            if let Some(fetched_logs) = self.logs_receiver.recv().await {
                if fetched_logs.batch_id == next_batch_id {
                    LogWriter::write_logs(&mut self.file, fetched_logs);
                    // Move to next batch ID
                    next_batch_id += 1;

                    // Some future batches may be writable now
                    while let Some(future_fetched_logs) = future_batches.remove(&next_batch_id) {
                        LogWriter::write_logs(&mut self.file, future_fetched_logs);
                        // Move to next batch ID
                        next_batch_id += 1;
                    }
                } else {
                    // If it's not the next batch ID, it's a future one so just store it for now
                    future_batches.insert(fetched_logs.batch_id, fetched_logs);
                }
            } else {
                println!("Logs receiver channel closed, stopping");
                return;
            }
        }
    }

    fn write_logs(file: &mut File, fetched_logs: FetchedLogs) {
        println!(
            "Batch ID {}, writing {} logs...",
            fetched_logs.batch_id,
            fetched_logs.logs.len()
        );
        for log in fetched_logs.logs {
            if let Err(e) = writeln!(file, "{}", serde_json::to_string(&log).unwrap()) {
                panic!("Couldn't write to file: {}", e);
            }
        }
    }
}
