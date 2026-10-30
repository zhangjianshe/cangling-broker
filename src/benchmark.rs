//! Isolated loopback gRPC regression and opt-in throughput benchmark.
use super::*;
use proto::message_queue_client::MessageQueueClient;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

async fn exercise(
    batch: usize,
    count: usize,
    topic_count: usize,
    wildcard: bool,
) -> (f64, f64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::connect(&format!("sqlite:{}/queue.db", dir.path().display()))
        .await
        .unwrap();
    let mut config = Config::test_default();
    config.claim_batch_size = batch;
    config.consumer_prefetch = 32;
    config.write_batch_size = 128;
    config.write_batch_wait_ms = 2;
    config.ack_timeout_secs = 30;
    let config = Arc::new(config);
    let (writer, writer_task) = QueueWriter::start(db.clone(), 8192, 128, Duration::from_millis(2))
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    let service = QueueService {
        db: db.clone(),
        writer: writer.clone(),
        config,
        subscribers: TopicSubscribers::default(),
        inflight: InflightAcks::default(),
        grpc_clients: GrpcClientRegistry::default(),
        shutdown: shutdown.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures_util::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let stop = shutdown.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(MessageQueueServer::new(service))
            .serve_with_incoming_shutdown(incoming, stop.cancelled())
            .await
            .unwrap();
    });
    let mut client = MessageQueueClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let topics = (0..topic_count)
        .map(|n| format!("benchmark/{n}"))
        .collect::<Vec<_>>();
    client
        .configure_topics(ConfigureTopicsRequest {
            topics: topics
                .iter()
                .map(|topic| ProtoTopicConfig {
                    topic: topic.clone(),
                    delivery: "single".into(),
                    persistence: "persistent".into(),
                })
                .collect(),
        })
        .await
        .unwrap();
    let publishes = (0..count)
        .map(|n| AcceptMessageRequest {
            idempotency_key: format!("message-{n}"),
            topic: topics[n % topic_count].clone(),
            payload: vec![42; 256],
            attributes: HashMap::new(),
        })
        .collect::<Vec<_>>();
    let mut accepted = client
        .accept_messages(tokio_stream::iter(publishes))
        .await
        .unwrap()
        .into_inner();
    let mut published_ids = HashSet::new();
    while let Some(message) = accepted.message().await.unwrap() {
        assert!(!message.duplicate);
        published_ids.insert(message.message_id);
    }
    assert_eq!(published_ids.len(), count);
    // Admission is asynchronous; wait for all writes before timing a fixed backlog drain.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let snapshot = db.status_snapshot(None).await.unwrap();
            if snapshot.iter().map(|t| t.pending).sum::<i64>() == count as i64 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let filters = if wildcard {
        vec!["benchmark/#".to_string()]
    } else {
        topics
    };
    let streams = filters.len();
    let started = Instant::now();
    let mut consumers = Vec::new();
    for filter in filters {
        let mut consumer = client.clone();
        let mut messages = consumer
            .subscribe(SubscribeRequest {
                topic: filter,
                consumer_id: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        consumers.push(tokio::spawn(async move {
            let mut ids = HashSet::new();
            let mut latencies = Vec::new();
            let mut acks = Vec::new();
            for n in 0..count / streams {
                let message = messages.message().await.unwrap().unwrap();
                assert_eq!(message.payload, vec![42; 256]);
                assert!(ids.insert(message.message_id.clone()), "duplicate delivery");
                let created = chrono::DateTime::parse_from_rfc3339(&message.created_at).unwrap();
                latencies.push(
                    (chrono::Utc::now() - created.with_timezone(&chrono::Utc))
                        .num_microseconds()
                        .unwrap() as f64
                        / 1000.0,
                );
                acks.push(AckMessageRequest {
                    message_id: message.message_id,
                    lease: message.lease,
                    success: true,
                    error: String::new(),
                });
                if acks.len() == 32 || n + 1 == count / streams {
                    let expected = acks.len() as u32;
                    let response = consumer
                        .ack_messages(AckMessagesRequest {
                            acknowledgements: std::mem::take(&mut acks),
                        })
                        .await
                        .unwrap()
                        .into_inner();
                    assert_eq!(response.accepted, expected);
                }
            }
            (ids, latencies)
        }));
    }
    let mut received_ids = HashSet::new();
    let mut latencies = Vec::new();
    for consumer in consumers {
        let (ids, times) = tokio::time::timeout(Duration::from_secs(60), consumer)
            .await
            .unwrap()
            .unwrap();
        received_ids.extend(ids);
        latencies.extend(times);
    }
    assert_eq!(received_ids, published_ids);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let snapshot = db.status_snapshot(None).await.unwrap();
            if snapshot.iter().map(|t| t.delivered).sum::<i64>() == count as i64 {
                assert_eq!(
                    snapshot
                        .iter()
                        .map(|t| t.pending + t.processing)
                        .sum::<i64>(),
                    0
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let throughput = count as f64 / started.elapsed().as_secs_f64();
    latencies.sort_by(f64::total_cmp);
    let result = (
        throughput,
        latencies[count / 2],
        latencies[count * 95 / 100],
    );
    drop(client);
    drop(accepted);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    writer.shutdown().await;
    writer_task.await.unwrap();
    db.close().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_batch_delivery_preserves_messages_and_counts() {
    exercise(16, 128, 1, false).await;
    exercise(16, 128, 4, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "isolated loopback throughput benchmark; run explicitly with --nocapture"]
async fn grpc_claim_batch_benchmark() {
    for (topics, wildcard) in [(1, false), (4, false), (4, true)] {
        for batch in [1, 8, 16, 32] {
            let (rate, p50, p95) = exercise(batch, 4000, topics, wildcard).await;
            println!("topics={topics} wildcard={wildcard} batch={batch} messages=4000 drain_per_sec={rate:.0} enqueue_to_receive_p50_ms={p50:.1} p95_ms={p95:.1}");
        }
    }
}
