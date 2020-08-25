const kafka = require("kafka-node");
const client = new kafka.KafkaClient("localhost:2181");

const Producer = kafka.Producer;
const producer = new Producer(client);

producer.on("ready", function () {
  console.log("Producer is ready");

  const sentMessage = "Hello !!!!";
  send(sentMessage);
});

function send(sentMessage) {
  payloads = [{ topic: "kafka-node-dev", messages: sentMessage, partition: 0 }];
  producer.send(payloads, function (err, data) {
    console.log("send data ", sentMessage);
  });
}
