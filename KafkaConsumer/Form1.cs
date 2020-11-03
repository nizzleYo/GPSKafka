using System;
using System.Linq;
using System.Threading;
using System.Windows.Forms;
using Confluent.Kafka;
using GMap.NET;
using GMap.NET.MapProviders;
using GMap.NET.WindowsForms;
using GMap.NET.WindowsForms.Markers;
using Newtonsoft.Json;

namespace KafkaConsumer
{
    public partial class Form1 : Form
    {
        private readonly object _overlayLock = new object();
        private GMapOverlay _markersOverlay = new GMapOverlay("markers");
        private Thread thread;

        public Form1()
        {
            InitializeComponent();
            map.DragButton = MouseButtons.Left;
            map.MapProvider = GMapProviders.GoogleMap;
            map.Position = new PointLatLng(48.8465,2.35156);
            map.MinZoom = 5;
            map.MaxZoom = 100;
            map.Zoom = 11;
            map.Overlays.Add(_markersOverlay);
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            thread = new Thread(KafkaConsume);
            thread.Start();
        }

        public void KafkaConsume()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                //consumer.Subscribe("streams-gpsdata-enriched");
                consumer.Subscribe("conprod-gpsdata-enriched");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        var payload = JsonConvert.DeserializeObject<GpsInput>(consumeResult.Message.Value);

                        var tag = payload.Unit.ToString();
                        var latitude = payload.Latitude;
                        var longitude = payload.Longitude;
                        var color = payload.Color;

                        AddOrUpdateMarker(tag, latitude, longitude, color);
                        Thread.Sleep(25);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }

        private void AddOrUpdateMarker(string tag, double lat, double lng, string color)
        {
            lock(_overlayLock)
            {           
                var markersOverlayTemp = _markersOverlay;
                var marker = markersOverlayTemp.Markers.FirstOrDefault(m => (string)m.Tag == tag);

                if (marker == null)
                {
                    var markerType = DetermineMarkerType(color);
                    marker = new GMarkerGoogle(new PointLatLng(lat, lng), markerType) {Tag = tag};
                    _markersOverlay.Markers.Add(marker);
                }

                marker.Position = new PointLatLng(lat, lng);
            }
        }

        private GMarkerGoogleType DetermineMarkerType(string color)
        {
            switch (color)
            {
                case "green":
                    return GMarkerGoogleType.green;
                case "blue":
                    return GMarkerGoogleType.blue;
                case "yellow":
                    return GMarkerGoogleType.yellow;
                case "purple":
                    return GMarkerGoogleType.purple;
                case "red":
                    return GMarkerGoogleType.red;
                case "orange":
                    return GMarkerGoogleType.orange;
                default:
                    return GMarkerGoogleType.pink;
            }
        }

        internal class GpsInput
        {
            public int Unit;
            public int Type;
            public double Latitude;
            public double Longitude;
            public string Color;
        }
    }
}
