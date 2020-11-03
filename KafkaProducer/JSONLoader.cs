using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace KafkaProducer
{
    class JsonLoader
    {
        public string path;

        public JsonLoader(string path)
        {
            this.path = path;
        }

        public List<OutputGps> LoadJson()
        {
            using (StreamReader r = new StreamReader(path))
            {
                string json = r.ReadToEnd();
                List<InputGps> items = JsonConvert.DeserializeObject<List<InputGps>>(json);
                List<OutputGps> outputGps = convertInputToOutput(items);
                return outputGps;
            }
        }

        private List<OutputGps> convertInputToOutput(List<InputGps> items)
        {
            var outputGps = items
                .Select(x => new OutputGps() { Unit = x.Unit, Type = x.Type, Latitude = x.Coordinates[1], Longitude = x.Coordinates[0] })
                .ToList();
            return outputGps;
        }
    }

    internal class InputGps
    {
        public int Unit;
        public int Type;
        public double[] Coordinates;
    }

    internal class OutputGps
    {
        public int Unit;
        public int Type;
        public double Latitude;
        public double Longitude;
    }
}
