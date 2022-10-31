use std::time::Duration;

use clap::Parser;
use model::User;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use rdkafka::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(short, long, default_value="localhost:9092")]
    broker: String,
    #[arg(short, long, default_value="5")]
    worker_count: usize,
    #[arg(short, long, default_value="filter")]
    group_id: String,
    from_topic: String,
    to_topic: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    (0..args.worker_count).map(|_| {
        tokio::spawn(filter(args.clone()))
    })
    .collect::<FuturesUnordered<_>>()
    .for_each(|_| async { () })
    .await;
}

async fn filter(args: Args) {
    let topics: Vec<&str> = vec![&args.from_topic];
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &args.group_id)
        .set("bootstrap.servers", &args.broker)
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&topics[..]).expect("Cannot subscribe.");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .create()
        .expect("Producer creation error");

    loop_masking_email(consumer, producer, args).await;
}

async fn loop_masking_email(consumer: StreamConsumer, producer: FutureProducer, args: Args) {
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let output_topic = args.to_topic.to_owned();
        async move {
            let user = borrowed_message.payload_view::<[u8]>().and_then(|result| {
                match User::deserialize(result.unwrap()) {
                    Ok(user) => Some(user),
                    Err(e) => {
                        println!("Cannot deserialize. Ignored {:?}.", e);
                        None
                    },
                }
            });

            if let Some(user) = user {
                tokio::spawn(async move {
                    let masked_user =
                        tokio::task::spawn_blocking(|| filter_email(user))
                        .await
                        .expect("failed to wait for expensive computation");
                
                    let bytes = User::serialize(&masked_user).expect("Cannot serialize user.");
                    producer.send(
                        FutureRecord::to(&output_topic)
                        .key(&masked_user.user_name)
                        .payload(&bytes),
                        Duration::ZERO,
                    ).await.expect("Kafka error");
                });
            }

            Ok(())
        }
    });
    stream_processor.await.expect("stream processing failed");
}

fn filter_email(mut user: User) -> User {
    user.email = "--- Masked ---".to_owned();
    user
}
