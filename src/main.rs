extern crate ctrlc;

use crate::mqtt_worker::MqttWorker;

mod mqtt_worker;

fn main() {
    println!("Hello, world!");
    let mqtt_worker = MqttWorker::new();

    let mqtt_worker2 = mqtt_worker.clone();

    ctrlc::set_handler(move || {
        mqtt_worker2.stop_consume();
    })
        .expect("Error setting Ctrl-C handler");

    mqtt_worker.subscribe_topics();

    // Just loop on incoming messages.
    // If we get a None message, check if we got disconnected,
    // and then try a reconnect.
    println!("\nWaiting for messages on topics {:?}...", mqtt_worker.get_sub_topics());
    for msg in mqtt_worker.get_receiver().iter() {
        if let Some(msg) = msg {
            println!("{}", msg);
            let content = "Received message from ".to_owned() + msg.topic() + ": " + &*msg.payload_str();
            mqtt_worker.publish_message(content);
        }
        else if mqtt_worker.is_connected() || mqtt_worker.try_reconnect() {
            break;
        }
    }

    // If we're still connected, then disconnect now,
    // otherwise we're already disconnected.
    if mqtt_worker.is_connected() {
        println!("\nDisconnecting...");
        mqtt_worker.unsubscribe_topics();
        mqtt_worker.disconnect();
    }
    println!("Exiting");
}