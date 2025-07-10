// <copyright file="CreateDataPacketHelper.cs" company="McLaren Applied Ltd.">
//
// Copyright 2024 McLaren Applied Ltd
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using MA.Streaming.API;
using MA.Streaming.OpenData;
using Xunit.Abstractions;

namespace MA.Streaming.IntegrationTests.Helper
{
    public class CreateDataPacketHelper
    {
        private string? dataSource;
        private DataFormatManagerService.DataFormatManagerServiceClient? dataFormatManager;
        private readonly ITestOutputHelper outputHelper;

        public CreateDataPacketHelper(string dataSource, DataFormatManagerService.DataFormatManagerServiceClient dataFormatManagerClient, ITestOutputHelper outputHelper) { 
            this.dataSource = dataSource;
            this.dataFormatManager = dataFormatManagerClient;
            this.outputHelper = outputHelper;
        }

        public async Task<RowDataPacket> CreateRowDataPacket(List<string> parameters, int randomSeed = 23456)
        {
            if (this.dataFormatManager is null)
            {
                throw new InvalidOperationException("Data format manager is null which shouldn't be");
            }

            var getParameterDataFormatIdRequest = new GetParameterDataFormatIdRequest
            {
                DataSource = this.dataSource,
            };
            getParameterDataFormatIdRequest.Parameters.Add(parameters);
            var dataFormatIdentifier = await this.dataFormatManager.GetParameterDataFormatIdAsync(getParameterDataFormatIdRequest);

            var row1Samples = new DoubleSampleList();
            var row2Samples = new DoubleSampleList();
            var random = new Random(randomSeed);

            for (int i = 0; i < parameters.Count; i++)
            {
                var sample1 = new DoubleSample
                {
                    Status = DataStatus.Valid,
                    Value = random.NextDouble()
                };
                var sample2 = new DoubleSample
                {
                    Status = DataStatus.Valid,
                    Value = 10 * (i + 1)
                };

                row1Samples.Samples.Add(sample1);
                row2Samples.Samples.Add(sample2);
            }

            var rowDataPacket = new RowDataPacket
            {
                DataFormat = new SampleDataFormat
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier
                },
                Timestamps = { 1732899802000123456, 1732899902000123456 },
                Rows =
            {
                new SampleRow
                {
                    DoubleSamples = row1Samples
                },
                new SampleRow
                {
                    DoubleSamples = row2Samples
                }
            }
            };

            var info = await this.dataFormatManager.GetParametersListAsync(
                new GetParametersListRequest
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                    DataSource = this.dataSource
                });
            var retryCounter = 0;
            while ((info.Parameters is null || info.Parameters.Count == 0) &&
                   retryCounter < 5)
            {
                if (retryCounter == 5)
                {
                    throw new InvalidOperationException("data format info not found");
                }
                retryCounter++;
                info = await this.dataFormatManager.GetParametersListAsync(
                    new GetParametersListRequest
                    {
                        DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                        DataSource = this.dataSource
                    });

                await Task.Delay(1000);
                this.outputHelper.WriteLine("retry");
            }

            return rowDataPacket;
        }

        public async Task<PeriodicDataPacket> CreatePeriodicDataPacket(List<string> parameters, int randomSeed = 23456)
        {
            if (this.dataFormatManager is null)
            {
                throw new InvalidOperationException("Data format manager is null which shouldn't be");
            }
            var getParameterDataFormatIdRequest = new GetParameterDataFormatIdRequest
            {
                DataSource = this.dataSource,
            };
            getParameterDataFormatIdRequest.Parameters.Add(parameters);
            var dataFormatIdentifier = await this.dataFormatManager.GetParameterDataFormatIdAsync(getParameterDataFormatIdRequest);



            var columns = new List<SampleColumn>();

            var random = new Random(randomSeed);

            for (int i = 0; i < parameters.Count; i++)
            {
                var column = new SampleColumn
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Status = DataStatus.Valid,
                                Value = random.NextDouble()
                            },
                            new DoubleSample
                            {
                                Status = DataStatus.Valid,
                                Value = 100*(i+1)
                            }
                        }
                    }
                };
                columns.Add(column);
            }

            var periodicDataPacket = new PeriodicDataPacket
            {
                DataFormat = new SampleDataFormat
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier
                },
                StartTime = 1732899679000123456,
                Interval = 2,
            };

            periodicDataPacket.Columns.Add(columns);

            var info = await this.dataFormatManager.GetParametersListAsync(
                new GetParametersListRequest
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                    DataSource = this.dataSource
                });
            var retryCounter = 0;
            while ((info.Parameters is null || info.Parameters.Count == 0) &&
                   retryCounter < 5)
            {
                if (retryCounter == 5)
                {
                    throw new InvalidOperationException("data format info not found");
                }
                retryCounter++;
                info = await this.dataFormatManager.GetParametersListAsync(
                    new GetParametersListRequest
                    {
                        DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                        DataSource = this.dataSource
                    });

                await Task.Delay(1000);
                this.outputHelper.WriteLine("retry");
            }

            return periodicDataPacket;
        }

        public async Task<SynchroDataPacket> CreateSynchroDataPacket(List<string> parameters, int randomSeed = 23456)
        {
            if (this.dataFormatManager is null)
            {
                throw new InvalidOperationException("Data format manager is null which shouldn't be");
            }
            var getParameterDataFormatIdRequest = new GetParameterDataFormatIdRequest
            {
                DataSource = this.dataSource,
            };
            getParameterDataFormatIdRequest.Parameters.Add(parameters);
            var dataFormatIdentifier = await this.dataFormatManager.GetParameterDataFormatIdAsync(getParameterDataFormatIdRequest);



            var columns = new List<SampleColumn>();

            var random = new Random(randomSeed);

            for (int i = 0; i < parameters.Count; i++)
            {
                var column = new SampleColumn
                {
                    DoubleSamples = new DoubleSampleList
                    {
                        Samples =
                        {
                            new DoubleSample
                            {
                                Status = DataStatus.Valid,
                                Value = random.NextDouble()
                            },
                            new DoubleSample
                            {
                                Status = DataStatus.Valid,
                                Value = 100*(i+1)
                            }
                            ,
                            new DoubleSample
                            {
                                Status = DataStatus.Valid,
                                Value = 10*(i+1)
                            }
                        }
                    }
                };
                columns.Add(column);
            }

            var synchroDataPacket = new SynchroDataPacket
            {
                DataFormat = new SampleDataFormat
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier
                },
                StartTime = 1732899679000123456,
                Intervals = { 500, 450 },
            };

            synchroDataPacket.Column.Add(columns);

            var info = await this.dataFormatManager.GetParametersListAsync(
                new GetParametersListRequest
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                    DataSource = this.dataSource
                });
            var retryCounter = 0;
            while ((info.Parameters is null || info.Parameters.Count == 0) &&
                   retryCounter < 5)
            {
                retryCounter++;
                info = await this.dataFormatManager.GetParametersListAsync(
                    new GetParametersListRequest
                    {
                        DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                        DataSource = this.dataSource
                    });
                if (retryCounter == 5)
                {
                    throw new InvalidOperationException("data format info not found");
                }

                await Task.Delay(1000);
                this.outputHelper.WriteLine("retry");
            }

            return synchroDataPacket;
        }

        public async Task<EventPacket> CreateEventPacket(string eventName)
        {
            if (this.dataFormatManager is null)
            {
                throw new InvalidOperationException("Data format manager is null which shouldn't be");
            }

            var dataFormatIdentifier = await this.dataFormatManager.GetEventDataFormatIdAsync(
                new GetEventDataFormatIdRequest
                {
                    DataSource = this.dataSource,
                    Event = eventName,
                }
            );

            var eventPacket = new EventPacket
            {
                DataFormat = new EventDataFormat
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier
                },
                Timestamp = 1732898779000123456,
                RawValues = { 209.4, 325.3, 0 }
            };

            var info = await this.dataFormatManager.GetEventAsync(
                new GetEventRequest
                {
                    DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                    DataSource = this.dataSource
                });
            var retryCounter = 0;
            while ((info.Event == "") &&
                   retryCounter < 5)
            {
                retryCounter++;
                info = await this.dataFormatManager.GetEventAsync(
                            new GetEventRequest
                            {
                                DataFormatIdentifier = dataFormatIdentifier.DataFormatIdentifier,
                                DataSource = this.dataSource
                            });
                if (retryCounter == 5)
                {
                    throw new InvalidOperationException("data format info not found");
                }

                await Task.Delay(1000);
                this.outputHelper.WriteLine("retry");
            }

            return eventPacket;
        }

    }
}
