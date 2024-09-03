#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MA.Streaming.Proto.ServerComponent.Host/MA.Streaming.Proto.ServerComponent.Host.csproj", "MA.Streaming.Proto.ServerComponent.Host/"]
COPY ["MA.Streaming.Proto.ServerComponent/MA.Streaming.Proto.ServerComponent.csproj", "MA.Streaming.Proto.ServerComponent/"]
COPY ["MA.Streaming.Proto.Core/MA.Streaming.Proto.Core.csproj", "MA.Streaming.Proto.Core/"]
COPY ["MA.Streaming.Abstraction/MA.Streaming.Abstraction.csproj", "MA.Streaming.Abstraction/"]
COPY ["MA.Streaming.Contracts/MA.Streaming.Contracts.csproj", "MA.Streaming.Contracts/"]
COPY ["MA.Streaming.Core/MA.Streaming.Core.csproj", "MA.Streaming.Core/"]

ARG NUGET_PAT
RUN dotnet nuget add source "https://nuget.pkg.github.com/mat-docs/index.json" --name MA --username "docker" --password "$NUGET_PAT" --store-password-in-clear-text

RUN dotnet restore "./MA.Streaming.Proto.ServerComponent.Host/MA.Streaming.Proto.ServerComponent.Host.csproj"
COPY . .
WORKDIR "/src/MA.Streaming.Proto.ServerComponent.Host"
RUN dotnet build "./MA.Streaming.Proto.ServerComponent.Host.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./MA.Streaming.Proto.ServerComponent.Host.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MA.Streaming.Proto.ServerComponent.Host.dll"]