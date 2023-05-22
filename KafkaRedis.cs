using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using StackExchange.Redis;

class Program
{
    static void Main(string[] args)
    {
        var config = new Dictionary<string, object>
        {
            { "bootstrap.servers", "localhost:9092" },
            { "group.id", "test-consumer-group" },
            { "auto.offset.reset", "earliest" }
        };

        var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8));
        consumer.Subscribe(new List<string> { "test-topic" });

        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase();

        while (true)
        {
            Message<Null, string> msg;
            if (consumer.Consume(out msg, TimeSpan.FromSeconds(1)))
            {
                Console.WriteLine($"Received message: {msg.Value}");
                db.StringSet("test-key", msg.Value);
            }
        }
    }
}
