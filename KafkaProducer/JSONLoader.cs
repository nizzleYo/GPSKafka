using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace KafkaProducer
{
    class JSONLoader
    {
        public string path;

        public JSONLoader(string path)
        {
            this.path = path;
        }

        public List<OutputGPS> LoadJson()
        {
            using (StreamReader r = new StreamReader(path))
            {
                string json = r.ReadToEnd();
                List<InputGPS> items = JsonConvert.DeserializeObject<List<InputGPS>>(json);
                List<OutputGPS> outputGPS = convertInputToOutput(items);
                return outputGPS;
            }
        }

        private List<OutputGPS> convertInputToOutput(List<InputGPS> items)
        {
            var outputGPS = items
                .Select(x => new OutputGPS() { unit = x.unit, type = x.type, latitude = x.coordinates[1], longitude = x.coordinates[0] })
                .ToList();
            return outputGPS;
        }
    }

    internal class InputGPS
    {
        public int unit;
        public int type;
        public double[] coordinates;
    }

    internal class OutputGPS
    {
        public int unit;
        public int type;
        public double latitude;
        public double longitude;
    }
}
