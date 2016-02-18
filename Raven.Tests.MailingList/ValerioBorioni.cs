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
using Raven.Json.Linq;
using System.Net;
using System.IO;
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
                    var all = session.Query<MyEntity>().Customize(r=> r.WaitForNonStaleResults()).ToList();
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
                var url = string.Format(@"http://localhost:8079/databases/{0}/studio-tasks/loadCsvFile", databaseName);
                var tempFile = Path.GetTempFileName() + ".csv";


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
                    var entity = session.Query<CsvEntity>().Customize(r=> r.WaitForNonStaleResults()).First();
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
                var uploadUrl = string.Format(@"http://localhost:8079/databases/{0}/studio-tasks/loadCsvFile", databaseName);
                var uploadTempFile = Path.GetTempFileName() + ".csv";


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
                var downloadTempFile = Path.GetTempFileName() + ".csv";

                using (var wc = new WebClient())
                {
                    wc.DownloadFile(downloadUrl, downloadTempFile);
                }

                Console.ReadLine();

                using (var file = new FileStream(downloadTempFile, FileMode.Open))
                {
                    using (var csvReader = new TextFieldParser(file))
                    {
                        csvReader.SetDelimiters(",");
                        var headers = csvReader.ReadFields();

                        Assert.Equal("@id", headers[0]);
                        Assert.Equal("Property_A", headers[1]);
                        Assert.Equal("@metadata", headers[2]);

                        var record = csvReader.ReadFields();
                        var json = record[2].TrimStart('\"').TrimEnd('\"').Replace("\"\"", "\"");
                        var metadata = RavenJObject.Parse(json);
                        metadata.Remove("Raven-Last-Modified");
                        metadata.Remove("Last-Modified");
                        metadata.Remove("@etag");

                        var areEquals = RavenJToken.DeepEquals(metadata, originalMetadata);

                        Assert.Equal(originalMetadata.ToString(), metadata.ToString());
                        Assert.True(areEquals);
                    }

                }

            }
        }


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
