use std::{env, process, thread, time::Duration};
use mqtt::Message;

extern crate paho_mqtt as mqtt;

const DFLT_BROKER:&str = "tcp://192.168.0.92:1883";
const DFLT_CLIENT:&str = "rust_publish";
const QOS:i32 = 1;

#[derive(Clone)]
pub struct MqttWorker<'a> {
    client: mqtt::Client,
    receiver: mqtt::Receiver<Option<Message>>,
    sub_topics: Vec<&'a str>,
    pub_topics: Vec<&'a str>,
    connected: bool
}

impl MqttWorker<'_> {
    pub fn new() -> MqttWorker<'static> {
        let host = env::args().nth(1).unwrap_or_else(|| DFLT_BROKER.to_string());

        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(host)
            .client_id(DFLT_CLIENT.to_string())
            .finalize();

        // Create a client.
        let cli = mqtt::Client::new(create_opts).unwrap_or_else(|err| {
            println!("Error creating the client: {:?}", err);
            process::exit(1);
        });

        let rx = cli.start_consuming();

        let lwt = mqtt::MessageBuilder::new()
            .topic("test")
            .payload("Consumer lost connection")
            .finalize();

        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .will_message(lwt)
            .finalize();

        // Connect and wait for it to complete or fail.
        if let Err(e) = cli.connect(conn_opts) {
            println!("Unable to connect:\n\t{:?}", e);
            process::exit(1);
        }

        MqttWorker {
            client: cli,
            receiver: rx,
            sub_topics: vec!["topic_2"],
            pub_topics: vec!["topic_1"],
            connected: true
        }
    }

    pub fn subscribe_topics(&self) {
        let qos = [1, 1];
        self.client.subscribe_many(&self.sub_topics.as_slice(), &qos)
            .and_then(|rsp| {
                rsp.subscribe_many_response()
                    .ok_or(mqtt::Error::General("Bad response"))
            })
            .and_then(|vqos| {
                println!("QoS granted: {:?}", vqos);
                Ok(())
            })
            .unwrap_or_else(|err| {
                println!("Error subscribing to topics: {:?}", err);
                self.client.disconnect(None).unwrap();
                process::exit(1);
            });
    }

    pub fn unsubscribe_topics(&self) {
        self.client.unsubscribe_many(&self.sub_topics.as_slice()).unwrap();
    }

    pub fn publish_message(&self, message: String) {
        for topic in self.pub_topics.iter() {
            let msg = mqtt::Message::new(topic.to_string(), message.clone(), QOS);
            let tok = self.client.publish(msg);
            if let Err(e) = tok {
                println!("Error sending message: {:?}", e);
                break;
            }
        }
    }

    pub fn try_reconnect(&self) -> bool
    {
        println!("Connection lost. Waiting to retry connection");
        for _ in 0..12 {
            thread::sleep(Duration::from_millis(5000));
            if self.client.reconnect().is_ok() {
                println!("Successfully reconnected");
                return true;
            }
        }
        println!("Unable to reconnect after several attempts.");
        false
    }

    pub fn disconnect(&self) {
        self.client.disconnect(None).unwrap();
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    pub fn stop_consume(&self) {
        self.client.stop_consuming();
    }

    pub fn get_pub_topics(&self) -> Vec<&str> {
        self.pub_topics.clone()
    }

    pub fn get_sub_topics(&self) -> Vec<&str> {
        self.sub_topics.clone()
    }

    pub fn get_receiver(&self) -> mqtt::Receiver<Option<Message>> {
        self.receiver.clone()
    }
}