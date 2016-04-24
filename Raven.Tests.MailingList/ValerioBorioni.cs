// -----------------------------------------------------------------------
//  <copyright file="AaronSt.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;
using System.Net;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Microsoft.VisualBasic.FileIO;

namespace Raven.Tests.MailingList
{
    public class ValerioBorioni : RavenTest
    {
        [Fact]
        public void RavenJValue_recognize_NAN_Float_isEqual_to_NAN_String()
        {
            using (var store = NewDocumentStore())
            {
                store.Configuration.RunInMemory = true;
                store.Initialize();


                using (var session = store.OpenSession())
                {
                    session.Store(new MyEntity());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.Query<MyEntity>().Customize(r => r.WaitForNonStaleResults()).ToList();
                    var changes = session.Advanced.WhatChanged();
                    Assert.Empty(changes);
                }
            };

        }

        public class MyEntity
        {
            public double Value { get; set; }

            public MyEntity()
            {
                Value = double.NaN;
            }
        }


        [Fact]
        public void Import_documents_by_csv_should_preserve_documentId_if_id_header_is_present()
        {
            var databaseName = "TestCsvDatabase";
            var entityName = typeof(CsvEntity).Name;
            var documentId = "MyCustomId123abc";

            using (var store = NewRemoteDocumentStore(false, null, databaseName))
            {
                var url = string.Format(@"http://localhost:8079/databases/{0}/studio-tasks/loadCsvFile", databaseName);
                var tempFile = Path.GetTempFileName();

                File.AppendAllLines(tempFile, new[]
                {
                    "id,Property_A,Value,@Ignored_Property," + Constants.RavenEntityName,
                    documentId +",a,123,doNotCare," + entityName
                });

                using (var wc = new WebClient())
                    wc.UploadFile(url, tempFile);

                using (var session = store.OpenSession(databaseName))
                {
                    var entity = session.Load<CsvEntity>(documentId);
                    Assert.NotNull(entity);

                    var metadata = session.Advanced.GetMetadataFor(entity);
                    var ravenEntityName = metadata.Value<string>(Constants.RavenEntityName);
                    Assert.Equal(entityName, ravenEntityName);
                }

            }
        }

        public class CsvEntity
        {
            public string Id { get; set; }
            public double Value { get; set; }

            public CsvEntity()
            {
                Value = double.NaN;
            }
        }

    }

    public class ValerioBorioni_csv_metadata : RavenTest
    {
        string databaseName = "TestCsvDatabase";
        string entityName = typeof(CsvEntity).Name;
        string documentId = "MyCustomId123abc";

        string json;
        RavenJObject originalMetadata;

        public ValerioBorioni_csv_metadata()
        {
            json = string.Format(@"
                {{
                    'Raven-Entity-Name': '{0}',
                    'Raven-Clr-Type': '{1}',
                    'Ensure-Unique-Constraints': [
                        {{
                            'Name': 'PropertyWithUniqueConstraint',
                            'CaseInsensitive': false
                        }}
                    ]
                }}", entityName, typeof(CsvEntity).FullName).Replace('\'', '\"');

            originalMetadata = RavenJObject.Parse(json);
        }


        [Fact]
        public void Import_documents_by_csv_should_preserve_metadata()
        {
            using (var store = NewRemoteDocumentStore(false, null, databaseName))
            {
                store.Conventions.FindTypeTagName = t => t.Name;

                var url = string.Format(@"http://localhost:8079/databases/{0}/studio-tasks/loadCsvFile", databaseName);
                var tempFile = Path.GetTempFileName();


                var csvJson = "\"" + json.Replace("\"", "\"\"") + "\"";

                File.AppendAllLines(tempFile, new[]
                {
                    "id,Property_A,Value,@Ignored_Property,metadata",
                    documentId +",a,123,doNotCare," + csvJson
                });

                using (var wc = new WebClient())
                    wc.UploadFile(url, tempFile);

                using (var session = store.OpenSession(databaseName))
                {
                    var entityByQyery = session.Query<CsvEntity>().Customize(r => r.WaitForNonStaleResults()).First();
                    Assert.NotNull(entityByQyery);

                    var entity = session.Load<CsvEntity>(documentId);
                    Assert.NotNull(entity);

                    var metadata = session.Advanced.GetMetadataFor(entity);
                    metadata.Remove("Raven-Last-Modified");
                    metadata.Remove("Last-Modified");
                    metadata.Remove("@etag");

                    var areEquals = RavenJToken.DeepEquals(metadata, originalMetadata);

                    Assert.Equal(originalMetadata.ToString(), metadata.ToString());
                    Assert.True(areEquals);
                }

            }
        }


        [Fact]
        public void Export_documents_to_csv_should_preserve_metadata()
        {
            using (var store = NewRemoteDocumentStore(true, null, databaseName))
            {
                store.Conventions.FindTypeTagName = t => t.Name;

                var uploadUrl = string.Format(@"http://localhost:8079/databases/{0}/studio-tasks/loadCsvFile", databaseName);
                var uploadTempFile = Path.GetTempFileName();


                var csvJson = "\"" + json.Replace("\"", "\"\"") + "\"";

                File.AppendAllLines(uploadTempFile, new[]
                {
                    "id,Property_A,Value,@Ignored_Property,metadata",
                    documentId +",a,123,doNotCare," + csvJson
                });

                using (var wc = new WebClient())
                    wc.UploadFile(uploadUrl, uploadTempFile);

                using (var session = store.OpenSession(databaseName))
                {
                    session.Query<CsvEntity>().Customize(r => r.WaitForNonStaleResults()).ToList();
                }

                var downloadUrl = string.Format(@"http://localhost:8079/databases/{0}/streams/query/Raven/DocumentsByEntityName?format=excel&download=true&query=Tag:{1}",
                databaseName, entityName);
                var downloadTempFile = Path.GetTempFileName();

                using (var wc = new WebClient())
                {
                    wc.DownloadFile(downloadUrl, downloadTempFile);
                }

                using (var file = new FileStream(downloadTempFile, FileMode.Open))
                {
                    using (var csvReader = new TextFieldParser(file))
                    {
                        csvReader.SetDelimiters(",");
                        var headers = csvReader.ReadFields();

                        Assert.Equal("@id", headers[0]);
                        Assert.Equal("@metadata", headers[1]);
                        Assert.Equal("Property_A", headers[2]);
                        Assert.Equal("Value", headers[3]);

                        var record = csvReader.ReadFields();
                        var json = record[1].TrimStart('\"').TrimEnd('\"').Replace("\"\"", "\"");
                        //Assert.Equal("", json);
                        var metadata = RavenJObject.Parse(json);
                        metadata.Remove("Raven-Last-Modified");
                        metadata.Remove("Last-Modified");
                        metadata.Remove("@etag");


                        var metadataProperty1_Ok = RavenJToken.DeepEquals(metadata["Raven-Entity-Name"], originalMetadata["Raven-Entity-Name"]);
                        Assert.True(metadataProperty1_Ok);

                        var metadataProperty2_Ok = RavenJToken.DeepEquals(metadata["Raven-Clr-Type"], originalMetadata["Raven-Clr-Type"]);
                        Assert.True(metadataProperty2_Ok);


                        var metadataProperty3_Ok = RavenJToken.DeepEquals(metadata["Ensure-Unique-Constraints"], originalMetadata["Ensure-Unique-Constraints"]);
                        Assert.True(metadataProperty3_Ok);


                        Assert.Equal(originalMetadata.ToString(), metadata.ToString());
                    }

                }

            }
        }

        //public static bool DeepContains(RavenJObject container, RavenJObject contained)
        //{
        //    foreach (var value in contained.Values)
        //    {
        //        if(value is RavenJArray)
        //        {

        //        }
        //        if(value is RavenJObject)
        //        {

        //        }
        //        if(value is RavenJValue)
        //        {
        //            var valueContainer = container.Value<RavenJValue>(value);
        //            var valueContained = (RavenJValue)value;

        //        }
        //        //var valueContainer = container.Value<RavenJObject>(value);
        //        //var valueContained = contained.Value<RavenJObject>(value);
        //        //var valueOk = DeepContains(valueContainer, valueContained);
        //        //if (valueOk == false)
        //        //    return false;
        //    }

        //    return true;
        //}


        public class CsvEntity
        {
            public string Id { get; set; }
            public double Value { get; set; }

            public string PropertyWithUniqueConstraint { get; set; }
            public CsvEntity()
            {
                Value = double.NaN;
            }
        }

    }

}
