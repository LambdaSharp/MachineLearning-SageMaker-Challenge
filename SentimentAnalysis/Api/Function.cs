using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.SageMakerRuntime;
using Amazon.SageMakerRuntime.Model;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace LambdaSharp.SentimentAnalysis.Api {

    //--- Classes ---
    internal class SageMakerBody {
        [JsonProperty("instances")]
        public IEnumerable<string> Instances { get; set; }
    }
    
    public static class StringExtensions {
        public static MemoryStream ToStream(this string str) {
            var memoryStream = new MemoryStream();
            var writer = new StreamWriter(memoryStream);
            writer.Write(str);
            writer.Flush();
            memoryStream.Position = 0;
            return memoryStream;
        }
        
        public static string AsString(this MemoryStream stream) {
            var reader = new StreamReader(stream);
            var str = reader.ReadToEnd();
            reader.Dispose();
            return str;
        }
    }
    
    public class Function : ALambdaFunction<APIGatewayProxyRequest, APIGatewayProxyResponse> {
        //--- Fields ---
        private string _sagemakerEndpoint;
        private IAmazonSageMakerRuntime _runtimeClient;

        //--- Methods ---
        public override async Task InitializeAsync(LambdaConfig config) {
            _sagemakerEndpoint = config.ReadText("EndpointName");
            _runtimeClient = new AmazonSageMakerRuntimeClient();
        }

        public override async Task<APIGatewayProxyResponse> ProcessMessageAsync(APIGatewayProxyRequest request, ILambdaContext context) {
        
            // TODO: add business logic
            LogInfo("Call the InvokeEndpoint method");
            LogInfo(_sagemakerEndpoint);
            LogInfo(request.Body);
            return new APIGatewayProxyResponse {
                StatusCode = 200,
                Body = "{\"response body goes here!\"}"
            };
        }
    }
}
