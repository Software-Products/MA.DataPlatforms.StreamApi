// <copyright file="MetricProviders.cs" company="McLaren Applied Ltd.">
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

using Prometheus;

namespace MA.Streaming.PrometheusMetrics
{
    public static class MetricProviders
    {
        private const string DataSource = "data_source";
        private const string SessionKeyLabel = "session_key";
        private const string ConnectionLabel = "connection";
        private const string StreamLabel = "stream";

        public static Counter NumberOfDataPacketsPublished { get; } = Metrics.CreateCounter(
            "stream_api_writer_number_published_data_packets",
            "Number of data packets published by stream api writer",
            DataSource,
            StreamLabel);

        public static Counter NumberOfInfoPacketsPublished { get; } = Metrics.CreateCounter(
            "stream_api_writer_number_published_essential_packets",
            "Number of info packets published by stream api writer",
            DataSource);

        public static Gauge NumberOfSessions { get; } = Metrics.CreateGauge(
            "stream_api_session_management_number_of_sessions",
            "Number of session stored in the stream api",
            DataSource);

        public static Gauge NumberOfDataFormats { get; } = Metrics.CreateGauge(
            "stream_api_data_format_management_data_formats",
            "Number of data format stored in the stream api",
            DataSource);

        public static Gauge NumberOfConnections { get; } = Metrics.CreateGauge(
            "stream_api_connection_management_number_of_active_connections",
            "Number of connections active connections in the stream api",
            DataSource);

        public static Counter NumberOfDataPacketRead { get; } = Metrics.CreateCounter(
            "stream_api_reader_management_number_of_read_data_packets",
            "Number of packets read and ready to deliver if write stream is active",
            ConnectionLabel,
            DataSource,
            StreamLabel);

        public static Counter NumberOfDataPacketDelivered { get; } = Metrics.CreateCounter(
            "stream_api_reader_management_number_of_delivered_data_packets",
            "Number of packets read and delivered by writing on the connection stream",
            ConnectionLabel,
            DataSource,
            StreamLabel);

        public static Counter NumberOfEssentialPacketRead { get; } = Metrics.CreateCounter(
            "stream_api_reader_management_number_of_read_essential_packets",
            "Number of packets read and ready to deliver if write stream is active",
            ConnectionLabel,
            DataSource);

        public static Counter NumberOfEssentialPacketDelivered { get; } = Metrics.CreateCounter(
            "stream_api_reader_management_number_of_delivered_essential_packets",
            "Number of packets read and delivered by writing on the connection stream",
            ConnectionLabel,
            DataSource);

        public static Counter NumberOfRoutedDataPackets { get; } = Metrics.CreateCounter(
            "stream_api_core_routed_data_packets",
            "Number of data packets routed by router",
            DataSource,
            StreamLabel);

        public static Counter NumberOfRoutedDataPacketsBytes { get; } = Metrics.CreateCounter(
            "stream_api_core_routed_data_packets_bytes",
            "Number of data packets routed by router",
            DataSource,
            StreamLabel);

        public static Counter NumberOfRoutedInfoPackets { get; } = Metrics.CreateCounter(
            "stream_api_core_essential_data_packets",
            "Number of essential packets routed by router",
            DataSource);

        public static Counter NumberOfRoutedInfoPacketsBytes { get; } = Metrics.CreateCounter(
            "stream_api_core_routed_data_packets_bytes",
            "Number of data packets routed by router",
            DataSource);

        public static Counter NumberOfReceivedMessagesFromRouter { get; } = Metrics.CreateCounter(
            "stream_api_core_number_of_message_data_packets",
            "Number of essential packets routed by router",
            ConnectionLabel,
            DataSource,
            StreamLabel,
            SessionKeyLabel);

        public static Counter NumberOfReceivedBytesFromRouter { get; } = Metrics.CreateCounter(
            "stream_api_core_number_of_message_data_packets",
            "Number of essential packets routed by router",
            ConnectionLabel,
            DataSource,
            StreamLabel,
            SessionKeyLabel);
    }
}
