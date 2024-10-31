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

namespace MA.Streaming.PrometheusMetrics;

public static class MetricProviders
{
    private const string DataSource = "data_source";
    private const string SessionKeyLabel = "session_key";
    private const string ConnectionLabel = "connection";
    private const string StreamLabel = "stream";

    public static Counter NumberOfDataPacketsPublished { get; } = Metrics.CreateCounter(
        "stream_api_data_packets_published_total",
        "Number of data packets published by stream api",
        DataSource,
        StreamLabel);

    public static Counter NumberOfInfoPacketsPublished { get; } = Metrics.CreateCounter(
        "stream_api_info_packets_published_total",
        "Number of info packets published by stream api",
        DataSource);

    public static Gauge NumberOfSessions { get; } = Metrics.CreateGauge(
        "stream_api_sessions_total",
        "Number of session available",
        DataSource);

    public static Gauge NumberOfDataFormats { get; } = Metrics.CreateGauge(
        "stream_api_data_formats_total",
        "Number of data format stored in the stream api",
        DataSource);

    public static Gauge NumberOfConnections { get; } = Metrics.CreateGauge(
        "stream_api_connections_total",
        "Number of connections active connections in the stream api",
        DataSource);

    public static Counter NumberOfDataPacketRead { get; } = Metrics.CreateCounter(
        "stream_api_data_packets_read_total",
        "Number of packets read and ready to deliver",
        ConnectionLabel,
        DataSource,
        StreamLabel);

    public static Counter NumberOfDataPacketDelivered { get; } = Metrics.CreateCounter(
        "stream_api_data_packets_delivered_total",
        "Number of packets delivered",
        ConnectionLabel,
        DataSource,
        StreamLabel);

    public static Counter NumberOfEssentialPacketRead { get; } = Metrics.CreateCounter(
        "stream_api_essential_packets_read_total",
        "Number of essential packets read and ready to deliver",
        ConnectionLabel,
        DataSource);

    public static Counter NumberOfEssentialPacketDelivered { get; } = Metrics.CreateCounter(
        "stream_api_essential_packets_delivered_total",
        "Number of packets delivered",
        ConnectionLabel,
        DataSource);

    public static Counter NumberOfRoutedDataPackets { get; } = Metrics.CreateCounter(
        "stream_api_data_packets_routed_total",
        "Number of data packets routed by router",
        DataSource,
        StreamLabel);

    public static Counter NumberOfRoutedDataPacketsBytes { get; } = Metrics.CreateCounter(
        "stream_api_data_packets_routed_bytes_total",
        "Total bytes of data packets routed by router",
        DataSource,
        StreamLabel);

    public static Counter NumberOfRoutedInfoPackets { get; } = Metrics.CreateCounter(
        "stream_api_info_packets_routed_total",
        "Number of info packets routed by router",
        DataSource);

    public static Counter NumberOfRoutedInfoPacketsBytes { get; } = Metrics.CreateCounter(
        "stream_api_info_packets_routed_bytes_total",
        "Total bytes of info packets routed by router",
        DataSource);

    public static Counter NumberOfReceivedMessagesFromRouter { get; } = Metrics.CreateCounter(
        "stream_api_router_messages_received_total",
        "Total number of messages received from router",
        ConnectionLabel,
        DataSource,
        StreamLabel,
        SessionKeyLabel);

    public static Counter NumberOfReceivedBytesFromRouter { get; } = Metrics.CreateCounter(
        "stream_api_router_messages_received_bytes_total",
        "Total message bytes received from router",
        ConnectionLabel,
        DataSource,
        StreamLabel,
        SessionKeyLabel);
}
