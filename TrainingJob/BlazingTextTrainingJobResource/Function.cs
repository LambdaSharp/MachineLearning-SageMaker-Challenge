using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.SageMaker;
using Amazon.SageMaker.Model;
using LambdaSharp.CustomResource;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace LambdaSharp.Challenge.TrainingJob.BlazingTextTrainingJobResource {

    public class RequestProperties {

        //--- Properties ---
        public string S3TrainData { get; set; }
        
        public string S3ValiadtionData { get; set; }
        
        public string S3Output { get; set; }
        
        public string ExecutionRoleArn { get; set; }
        

    }

    public class ResponseProperties {

        //--- Properties ---
        public string DataOutput { get; set; }
        public string JobName { get; set; }
        
    }

    public class Function : ALambdaCustomResourceFunction<RequestProperties, ResponseProperties> {
        private IAmazonSageMaker _sagemaker;
        private IAmazonS3 _s3;
        private string _id;
        private string _region;
        private readonly IDictionary<string, string> RegionMap = new Dictionary<string, string> {
            { "us-east-1", "811284229777" },
            { "us-east-2", "825641698319" },
            { "us-west-2", "433757028032" },
            { "eu-west-1", "685385470294" },
            { "eu-central-1", "813361260812" },
            { "ap-northeast-1", "501404015308" },
            { "ap-northeast-2", "306986355934" },
            { "ap-southeast-2", "544295431143" },
            { "ap-southeast-1", "475088953585" },
            { "ap-south-1", "991648021394" },
            { "ca-central-1", "469771592824" },
            { "eu-west-2", "644912444149" },
            { "us-west-1", "632365934929" },
        };

        //--- Methods ---
        public override Task InitializeAsync(LambdaConfig config) {
            _sagemaker = new AmazonSageMakerClient();
            _s3 = new AmazonS3Client();
            _region = System.Environment.GetEnvironmentVariable("AWS_REGION");
            _id = RegionMap[_region];
            return Task.CompletedTask;
        } 

        protected override async Task<Response<ResponseProperties>> HandleCreateResourceAsync(Request<RequestProperties> request) {
            var props = request.ResourceProperties;
            var time = DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss");
            var jobName = $"blazingtext-{time}";
            var trainingImage = $"{_id}.dkr.ecr.{_region}.amazonaws.com/blazingtext:latest";
            var response = await _sagemaker.CreateTrainingJobAsync(
                new CreateTrainingJobRequest {
                    TrainingJobName = jobName,
                    AlgorithmSpecification = new AlgorithmSpecification {
                        TrainingImage = trainingImage,
                        TrainingInputMode = new TrainingInputMode("File"),
                    },
                    RoleArn = props.ExecutionRoleArn,
                    ResourceConfig = new ResourceConfig {
                        InstanceCount = 1,
                        InstanceType = TrainingInstanceType.MlC54xlarge,
                        VolumeSizeInGB = 30,
                    },
                    StoppingCondition = new StoppingCondition {MaxRuntimeInSeconds = 3600},
                    HyperParameters = new Dictionary<string, string> {
                        {"mode", "supervised"},
                        {"epochs", "10"},
                        {"min_count", "2"},
                        {"learning_rate", "0.05"},
                        {"vector_dim", "10"},
                        {"early_stopping", "true"},
                        {"patience", "4"},
                        {"min_epochs", "5"},
                        {"word_ngrams", "2"}
                    },
                    InputDataConfig = new List<Channel> {
                        new Channel {
                            ContentType = "text/plain",
                            DataSource = new DataSource {
                                S3DataSource = new S3DataSource {
                                    S3DataDistributionType = new S3DataDistribution("FullyReplicated"),
                                    S3DataType = new S3DataType("S3Prefix"),
                                    S3Uri = props.S3TrainData
                                }
                            },
                            ChannelName = "train"
                        },
                        new Channel {
                            ContentType = "text/plain",
                            DataSource = new DataSource {
                                S3DataSource = new S3DataSource {
                                    S3DataDistributionType = new S3DataDistribution("FullyReplicated"),
                                    S3DataType = new S3DataType("S3Prefix"),
                                    S3Uri = props.S3ValiadtionData
                                }
                            },
                            ChannelName = "validation"
                        }
                    },
                    OutputDataConfig = new OutputDataConfig {
                        S3OutputPath = props.S3Output
                    },
                }
            );
            Console.WriteLine(response.TrainingJobArn);
            
            // check for job completion before returning
            DescribeTrainingJobResponse desc;
            do {
                desc = _sagemaker.DescribeTrainingJobAsync(new DescribeTrainingJobRequest {
                    TrainingJobName = jobName
                }).Result;
                Console.WriteLine($"Training Status: {desc.TrainingJobStatus}");
                Console.WriteLine($"{desc.SecondaryStatus}");
                Thread.Sleep(30000);
            } while (desc.TrainingJobStatus == TrainingJobStatus.InProgress);
            
            // If the job fails, throw an exception with the failure reason
            if(desc.TrainingJobStatus == TrainingJobStatus.Failed) {
                throw new Exception(desc.FailureReason);
            }
            Console.WriteLine(desc.OutputDataConfig.S3OutputPath);
            desc.FinalMetricDataList.ForEach(x => LogInfo($"{x.MetricName}: {x.Value}"));
            return new Response<ResponseProperties> {

                // assign a physical resource ID to custom resource
                PhysicalResourceId = desc.OutputDataConfig.S3OutputPath,

                // set response properties
                Properties = new ResponseProperties {
                    DataOutput = $"{desc.OutputDataConfig.S3OutputPath}{jobName}/output/model.tar.gz",
                    JobName = jobName
                }
            };
        }

        protected override async Task<Response<ResponseProperties>> HandleDeleteResourceAsync(Request<RequestProperties> request) {
        
            // Training jobs cannot be deleted once they have been completed
            return new Response<ResponseProperties>();
        }

        protected override async Task<Response<ResponseProperties>> HandleUpdateResourceAsync(Request<RequestProperties> request) {
            return await HandleCreateResourceAsync(request);
        }
    }
}
