using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Xml;
using Newtonsoft.Json;
using NLog;

namespace RecycleBin
{
    class RecycleBin
    {
        public static HttpClient httpclient;

        public static Logger logger = LogManager.GetCurrentClassLogger();

        public static bool useIntegratedSecurity;
        public static string proxy;

        // SQLStatement applies a SQL statement as direct input or via stored procedure.
        static void SQLstatement(string dbserver, string database, string type)
        {
            string connectionString = Helpers.GetConnectionString(dbserver, database, useIntegratedSecurity);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand { Connection = connection, CommandTimeout = 0 })
                {
                    try
                    {
                        if (type == "truncate")
                        {
                            cmd.CommandText = "[DMAS].[dbo].[st_truncate_table]";
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.Parameters.Add(new SqlParameter("@Tablename", "[DMAS].[dbo].[TB_ODS_CAAGILE_RECYCLEBIN]"));
                            cmd.ExecuteNonQuery();
                        }
                        else if (type == "delete")
                        {
                            cmd.CommandText = Helpers.RequireSetting("SQLQuery");
                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error(e, "Error in SqlStatement ({0})", type);
                        throw;
                    }
                }
            }
        }

        static void LoadDataset(DataSet dataset, int pagesize, string urlbase)
        {
            string dbserver = Helpers.RequireSetting("dbserver");
            string database = Helpers.RequireSetting("database");
            string connString = Helpers.GetConnectionString(dbserver, database, useIntegratedSecurity);

            string urlCount = urlbase + "&fetch=ObjectUUID&start=1&pagesize=1";
            string jsonCount = Helpers.WebRequestWithToken(httpclient, urlCount);
            string[] tokens = jsonCount.Split(',');
            int totalResultCount = Convert.ToInt32(Regex.Match(tokens[4], @"\d+").Value);
            double nbIteration = Math.Ceiling((double)totalResultCount / pagesize);

            int start = 1;

            for (int i = 1; i <= nbIteration; i++)
            {
                string url = urlbase + "&fetch=ObjectUUID&start=" + start + "&pagesize=" + pagesize;

                Console.WriteLine("start : {0} **** PageSize : {1}", start, pagesize);

                string json = Helpers.WebRequestWithToken(httpclient, url);
                try
                {
                    XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);

                    foreach (DataTable dataTable in dataset.Tables)
                        dataTable.BeginLoadData();

                    dataset.ReadXml(new XmlTextReader(new StringReader(doc.OuterXml)));

                    foreach (DataTable dataTable in dataset.Tables)
                        dataTable.EndLoadData();
                }
                catch (Exception e)
                {
                    logger.Error(e, "Error deserializing page starting at {0}", start);
                }

                DataTable recycleBin = new DataTable();
                recycleBin.Columns.Add(new DataColumn
                {
                    ColumnName = "FormattedID",
                    DataType = typeof(string)
                });

                try
                {
                    foreach (DataRow rw in dataset.Tables["Results"].Rows)
                    {
                        DataRow newRow = recycleBin.NewRow();
                        newRow["FormattedID"] = rw["ObjectUUID"];
                        recycleBin.Rows.Add(newRow);
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e, "Error reading the Results table for page starting at {0}", start);
                }

                dataset.Clear();

                using (SqlConnection connection = new SqlConnection(connString))
                {
                    connection.Open();
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection))
                    {
                        bulkcopy.DestinationTableName = "[DMAS].[dbo].[TB_ODS_CAAGILE_RECYCLEBIN]";
                        bulkcopy.WriteToServer(recycleBin);
                    }
                }

                start = start + pagesize;
            }
        }

        static int Main(string[] args)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            logger.Info("Processing RecycleBin has started");

            try
            {
                useIntegratedSecurity = bool.Parse(Helpers.RequireSetting("IntegratedSecurity"));
                proxy = Helpers.RequireSetting("proxy");
                string dbserver = Helpers.RequireSetting("dbserver");
                string database = Helpers.RequireSetting("database");
                int blockSize = int.Parse(Helpers.RequireSetting("BlockSize"));

                httpclient = Helpers.WebAuthenticationWithToken(proxy);

                string usUrl = "https://eu1.rallydev.com/slm/webservice/v2.0/hierarchicalrequirement?workspace=https://eu1.rallydev.com/slm/webservice/v2.0/workspace/65088890229";
                string feUrl = "https://eu1.rallydev.com/slm/webservice/v2.0/portfolioitem/feature?workspace=https://eu1.rallydev.com/slm/webservice/v2.0/workspace/65088890229";

                // userstories

                logger.Info("Processing RecycleBin for userstories has started");

                SQLstatement(dbserver, database, "truncate");

                LoadDataset(new DataSet(), blockSize, usUrl);

                logger.Info("Processing RecycleBin for userstories has been completed");

                // features

                logger.Info("Processing RecycleBin for features has started");

                LoadDataset(new DataSet(), blockSize, feUrl);

                SQLstatement(dbserver, database, "delete");

                logger.Info("Processing RecycleBin for features has been completed");

                logger.Info("Processing RecycleBin has been completed");
                return 0;
            }
            catch (Exception e)
            {
                logger.Error(e, "Processing RecycleBin has failed");
                return 1;
            }
            finally
            {
                if (httpclient != null)
                    httpclient.Dispose();
            }
        }
    }
}
