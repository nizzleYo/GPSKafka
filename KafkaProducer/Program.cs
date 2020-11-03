using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading;

namespace KafkaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            JsonLoader json = new JsonLoader("C:\\Users\\nms\\source\\repos\\GPSKafka\\KafkaProducer\\data_paris_service_alpha_nocolor.json");
            var parisData = json.LoadJson();

            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            Action<DeliveryReport<Null, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivered message to {r.TopicPartitionOffset}"
                    : $"Delivery Error: {r.Error.Reason}");

            using (var producerBuilder = new ProducerBuilder<Null, string>(config).Build())
            {
                foreach (var gpsData in parisData)
                {
                    string payload = JsonConvert.SerializeObject(gpsData);
                    producerBuilder.Produce("raw-gpsdata-unenriched", new Message<Null, string> { Value = payload }, handler);
                    //Thread.Sleep(100);
                }

                producerBuilder.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
