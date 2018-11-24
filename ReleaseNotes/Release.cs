using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ReleaseNotes
{
    public class WorkItem
    {
        [JsonProperty("System.WorkItemType")] public string Type { get; set; }
        [JsonProperty("System.Title")] public string Title { get; set; }
    }

    public class FlymarkRelease : TableEntity
    {
        public List<ReleaseNoteItem> ReleaseNotes { get; set; }
        public bool Latest { get; set; }
        public bool Unstable { get; set; }
        public bool ShowInLog { get; set; }
        public string VersionStamp { get; set; }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var items = base.WriteEntity(operationContext);
            items[nameof(ReleaseNotes)] = new EntityProperty(JsonConvert.SerializeObject(ReleaseNotes));
            return items;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            if (properties.ContainsKey(nameof(ReleaseNotes)))
            {
                ReleaseNotes =
                    JsonConvert.DeserializeObject<List<ReleaseNoteItem>>(properties[nameof(ReleaseNotes)].StringValue);
            }
        }
    }

    public class ReleaseNoteItem
    {
        public string Description { get; set; }
        public string Type { get; set; }
    }

    public static class ReleaseNewVersion
    {
        private const string StorageConnectionString = "AzureWebJobsStorage";
        private const string TableName = "releases";
        private const string Container = "releases";

        [FunctionName("release-new-version")]
        [return: Table(TableName, Connection = StorageConnectionString)]
        public static async Task<FlymarkRelease> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "release-new-version/{program}")]
            HttpRequestMessage req,
            string program,
            ILogger log)
        {
            log.LogInformation("Start new release");
            var requestBody = await req.Content.ReadAsStringAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            var workItems = ((JArray) data.SelectToken("resource.data.workItems"))
                .Select(j => j["fields"].ToObject<WorkItem>())
                .Select(t => new ReleaseNoteItem {Description = t.Title, Type = t.Type});
            var version = BuildVersionNumberFromMetaData(data);

            return new FlymarkRelease
            {
                PartitionKey = program,
                VersionStamp = version,
                RowKey = ConvertVersionToRowKey(version),
                ReleaseNotes = workItems.ToList(),
                Unstable = true,
                ShowInLog = true
            };
        }

        public static string ConvertVersionToRowKey(string version)
        {
            int.TryParse(version.Replace(".", ""), out var numbericVersion);
            return (int.MaxValue - numbericVersion).ToString();
        }

        private static string BuildVersionNumberFromMetaData(dynamic data)
        {
            return ((JToken) data.SelectToken("resource.environment.release.name")).ToString();
        }

        [FunctionName("release-validate-version")]
        public static async Task<HttpResponseMessage> RunValidateVersion(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "release-validate-version/{program}/{version}")]
            HttpRequestMessage req,
            string program,
            string version,
            [Table(TableName)] CloudTable table,
            [Blob(Container, FileAccess.Read, Connection = StorageConnectionString)]
            CloudBlobContainer container)
        {
            var stable = await LatestStableAsync(table, program);
            if (stable.VersionStamp == version)
            {
                return req.CreateResponse(HttpStatusCode.OK, new FlymarkVersionCheck
                {
                    IsLatest = true
                });
            }

            var userVersion = await UserVersionAsync(table, program, version);

            var urlToDownload = UrlToDownload(program, container, version);
            return req.CreateResponse(HttpStatusCode.OK, new FlymarkVersionCheck
            {
                IsLatest = false,
                IsUnstable = userVersion?.Unstable ?? false,
                LatestVersion = stable.RowKey,
                Download = urlToDownload
            });
        }


        [FunctionName("release-change-log")]
        public static async Task<HttpResponseMessage> ChangeLog(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "release-change-log/{program}")]
            HttpRequestMessage req,
            string program,
            [Table(TableName)] CloudTable table,
            [Blob(Container, FileAccess.Read, Connection = StorageConnectionString)]
            CloudBlobContainer container)
        {
            var changelog = await ReadChangeLogAsync(table, program);
            var transformed = changelog
                .Select(c => new
                {
                    Version = c.RowKey,
                    ReleaseNotes = BuildReleaseNoteHtml(c.ReleaseNotes),
                    Date = c.Timestamp.Date.ToShortDateString(),
                    c.Unstable,
                    c.VersionStamp,
                    c.Latest,
                    DownloadUrl = c.Latest || c.Unstable
                        ? UrlToDownload(program, container, c.VersionStamp)
                        : string.Empty
                });
            return req.CreateResponse(HttpStatusCode.OK, transformed);
        }

        private static dynamic BuildReleaseNoteHtml(List<ReleaseNoteItem> releaseNoteItems)
        {
            return releaseNoteItems
                .GroupBy(r => r.Type)
                .OrderBy(c => c.Key == "Bug" ? 0 : 1)
                .Select(c => new
                {
                    Type = c.Key == "Bug" ? "Bug fixes" : "New features",
                    Changes = c.Select(ch => ch.Description)
                });
        }

        private static string UrlToDownload(string key, CloudBlobContainer container, string version)
        {
            if (key == "server")
            {
                var adHocSas = new SharedAccessBlobPolicy
                {
                    SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
                    Permissions = SharedAccessBlobPermissions.Read
                };
                var blob = container.GetBlockBlobReference($"Server.{version}.zip");
                var sasBlobToken = blob.GetSharedAccessSignature(adHocSas);

                return blob.Uri + sasBlobToken;
            }

            return string.Empty;
        }

        private static async Task<List<FlymarkRelease>> ReadChangeLogAsync(CloudTable table, string key)
        {
            var query = new TableQuery<FlymarkRelease>()
                .Where(TableQuery.GenerateFilterCondition(nameof(FlymarkRelease.PartitionKey), QueryComparisons.Equal,
                    key.ToLower()));

            var result = await table.ExecuteQuerySegmentedAsync(query, null);
            return result.Results;
        }

        private static async Task<FlymarkRelease> LatestStableAsync(CloudTable table, string key)
        {
            var query = new TableQuery<FlymarkRelease>()
                .Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(nameof(FlymarkRelease.PartitionKey), QueryComparisons.Equal,
                        key.ToLower()),
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForBool(nameof(FlymarkRelease.Latest), QueryComparisons.Equal,
                        true)));

            var result = await table.ExecuteQuerySegmentedAsync(query, null);
            return result.First();
        }

        private static async Task<FlymarkRelease> UserVersionAsync(CloudTable table, string key, string version)
        {
            var rowKey = ConvertVersionToRowKey(version);
            var query = new TableQuery<FlymarkRelease>()
                .Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(nameof(FlymarkRelease.PartitionKey), QueryComparisons.Equal,
                        key.ToLower()),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(nameof(FlymarkRelease.RowKey), QueryComparisons.Equal, rowKey)));

            var result = await table.ExecuteQuerySegmentedAsync(query, null);
            return result.FirstOrDefault();
        }
    }

    public class FlymarkVersionCheck
    {
        public bool IsLatest { get; set; }
        public bool IsUnstable { get; set; }
        public string LatestVersion { get; set; }
        public string Download { get; set; }
    }
}