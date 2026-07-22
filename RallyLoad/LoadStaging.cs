using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;

namespace LoadStaging
{
    class LoadStaging
    {
        private static HttpClient httpclient;

        public static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static bool useIntegratedSecurity;
        private static string proxy;
        private static string SqlSchema;

        // number of table load tasks that failed during the run
        private static int _failedTables;

        // SQLStatement takes a list of tables and apply SQL statement as direct input or via Stored procedure
        static void SQLstatement(string dbserver, string database, string[] listtable, bool is_stagging)
        {
            string connectionString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlCommand cmd = new SqlCommand { Connection = connection, CommandTimeout = 0 })
                {
                    try
                    {
                        if (is_stagging == true)
                        {
                            foreach (var t in listtable)
                            {
                                // table names come from configuration; allow-list validated before use
                                string safeTable = Tools.ValidateIdentifier(t, "listtable");
                                cmd.CommandText = "[DMAS].[dbo].[st_truncate_table]";
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.Parameters.Clear();
                                cmd.Parameters.Add(new SqlParameter("@Tablename", string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + safeTable + "]"));
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error(e, "Error in Sqlstatement (truncate tables)");
                        throw;
                    }
                }
            }
        }

        // method that explores a dataset and loads into SQL Server tables the related data
        // 'listexception' is used to park all the exceptions due to structure redefinition (column add/delete) at certain point of time
        static void Bulkinsertdynamic(DataSet dataset, DataTable datatable, string dbserver, string database, string rootnode)
        {
            // rootnode is embedded into SQL object names; allow-list validate it
            rootnode = Tools.ValidateIdentifier(rootnode, "rootnode");

            string[] listtable = Tools.ReadListConfiguration(datatable, "Bulkinsert", rootnode);
            string[] listmappedcolumn = Tools.ReadListConfiguration(datatable, "mapping", rootnode);

            string connString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connString))
            {
                bulkcopy.BulkCopyTimeout = 0;
                int i = 0;

                DataTable inputDataTableMapping = new DataTable();
                DataTable ResultDataTableMapping = new DataTable();
                for (int count = 0; count <= dataset.Tables.Count - 1; count++)
                {
                    if (listtable.Contains(dataset.Tables[count].TableName.ToString()))
                    {
                        // table names originate from the remote API payload; allow-list validate
                        // before they are embedded into SQL object names
                        string safeDataTableName;
                        try
                        {
                            safeDataTableName = Tools.ValidateIdentifier(dataset.Tables[count].TableName, "TableName");
                        }
                        catch (ArgumentException e)
                        {
                            logger.Warn(e, "table : {0} -- Skipping table with invalid name", rootnode);
                            continue;
                        }

                        inputDataTableMapping = dataset.Tables[count];
                        if (i == 0)
                        {
                            ResultDataTableMapping = Tools.CompareRows(Tools.GetSQLTable(dbserver, database, "TB_STG_CAAGILE_" + rootnode.ToUpper()), Tools.GetAGTable(inputDataTableMapping), inputDataTableMapping, listmappedcolumn);
                            bulkcopy.DestinationTableName = string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + rootnode.ToUpper() + "]";
                        }
                        else
                        {
                            ResultDataTableMapping = Tools.CompareRows(Tools.GetSQLTable(dbserver, database, "TB_STG_CAAGILE_" + rootnode.ToUpper() + "_" + safeDataTableName), Tools.GetAGTable(inputDataTableMapping), inputDataTableMapping, listmappedcolumn);
                            bulkcopy.DestinationTableName = string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + rootnode.ToUpper() + "_" + safeDataTableName + "]";
                        }

                        bulkcopy.ColumnMappings.Clear();

                        int length = ResultDataTableMapping.Columns.Count;

                        for (int k = length - 1; k >= 0; k--)
                        {
                            try
                            {
                                bulkcopy.ColumnMappings.Add(ResultDataTableMapping.Columns[k].ColumnName.Trim(), ResultDataTableMapping.Columns[k].ColumnName.Trim());
                            }
                            catch (Exception e)
                            {
                                logger.Warn(e, "table : {0} -- Error in BulkInsert column mapping", rootnode);
                            }
                        }

                        try
                        {
                            bulkcopy.WriteToServer(ResultDataTableMapping);
                        }
                        catch (Exception e)
                        {
                            // one failing table must not stop the other tables of the same page
                            logger.Error(e, "table : {0} -- Error in bulkcopy", rootnode);
                        }

                        i++;
                    }
                }
            }
        }

        // retrieve feature creator name from feature revision history
        static void UpdateDatatable()
        {
            string dbserver = Tools.RequireSetting("dbserver");
            string database = Tools.RequireSetting("database");

            string json;
            string url;

            string connString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);

            // load datatable Feature RevisionHistory
            DataTable dataTable = new DataTable();

            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = string.Format("Select * from [DMAS].[{0}].[TB_STG_CAAGILE_FEATURE_RevisionHistory]", SqlSchema);
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(dataTable);
                    da.Dispose();
                }
            }

            int counter = 0;
            foreach (DataRow dr in dataTable.Rows)
            {
                url = dr["_ref"].ToString() + "/Revisions?query=(RevisionNumber = \"0\")";
                json = Tools.WebRequestWithToken(httpclient, url);
                JObject rss = JObject.Parse(json);
                string FeatureCreatorID = (string)rss.SelectToken("QueryResult.Results[0].User._refObjectUUID");
                if (!String.IsNullOrEmpty(FeatureCreatorID))
                {
                    dr.BeginEdit();
                    dr["_type"] = FeatureCreatorID.ToString();
                    dr.EndEdit();
                    counter++;
                }
            }

            // insert Revisions datatable into SQL table Revisions
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();

                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = "[DMAS].[dbo].[st_truncate_table]";
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add(new SqlParameter("@Tablename", string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_FEATURE_RevisionHistory]", SqlSchema)));
                    cmd.ExecuteNonQuery();
                }

                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection))
                {
                    bulkcopy.DestinationTableName = string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_FEATURE_RevisionHistory]", SqlSchema);
                    bulkcopy.WriteToServer(dataTable);
                }
                dataTable.Clear();
            }

            logger.Info("# {0} Feature IDs were created", counter);
        }

        static void GetMilestones(DataTable ConfigTabletoLoad, string table, string PortfolioItemType)
        {
            // table is embedded into SQL object names; allow-list validate it
            table = Tools.ValidateIdentifier(table, "table");

            string dbserver = Tools.RequireSetting("dbserver");
            string database = Tools.RequireSetting("database");

            string json;
            string url;

            string connString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);

            // load datatable Feature RevisionHistory
            DataTable MilestonesRef = new DataTable();

            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = string.Format("SELECT M._ref as url, P.ObjectUUID as PortfolioItemID FROM [DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + table.ToUpper() + "_Milestones] M " +
                               string.Format("INNER JOIN [DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + table.ToUpper() + "] P ON (P.Results_Id = M.Results_Id) " +
                               "WHERE count > 0";
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(MilestonesRef);
                    da.Dispose();
                }
            }

            DataSet dataset = new DataSet();
            bool FirstTime = true;
            foreach (DataRow dr in MilestonesRef.Rows)
            {
                int currentIndex = 0;

                url = dr["url"].ToString() + "?pagesize=100";
                json = Tools.WebRequestWithToken(httpclient, url);
                try
                {
                    XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);

                    foreach (DataTable dt in dataset.Tables)
                        dt.BeginLoadData();

                    using (StringReader xmlInput = new StringReader(doc.OuterXml))
                    using (XmlReader xmlReader = Tools.CreateSecureXmlReader(xmlInput))
                    {
                        dataset.ReadXml(xmlReader);
                    }

                    foreach (DataTable dt in dataset.Tables)
                        dt.EndLoadData();
                }
                catch (Exception e)
                {
                    logger.Error(e, "An error occurred in load Datasets - DeserializeXmlNode");
                }

                DataTable resultsTable = dataset.Tables["Results"];
                if (resultsTable == null)
                {
                    logger.Warn("No 'Results' table found in milestones page; skipping this portfolio item.");
                    dataset.Clear();
                    continue;
                }

                if (FirstTime)
                {
                    if (!resultsTable.Columns.Contains("PortfolioItemID"))
                    {
                        resultsTable.Columns.Add(new DataColumn
                        {
                            ColumnName = "PortfolioItemID",
                            DataType = typeof(string)
                        });
                    }
                    if (!resultsTable.Columns.Contains("PortfolioItemType"))
                    {
                        resultsTable.Columns.Add(new DataColumn
                        {
                            ColumnName = "PortfolioItemType",
                            DataType = typeof(string)
                        });
                    }

                    FirstTime = false;
                }

                foreach (DataRow rw in resultsTable.Rows)
                {
                    resultsTable.Rows[currentIndex]["PortfolioItemID"] = dr["PortfolioItemID"];
                    resultsTable.Rows[currentIndex]["PortfolioItemType"] = PortfolioItemType;
                    currentIndex++;
                }

                Bulkinsertdynamic(dataset, ConfigTabletoLoad, dbserver, database, "milestones");
                dataset.Clear();
            }

            logger.Info("Milestones inserted into SQL");
        }

        // retrieve selected revision history items from epic revision history
        static void GetRevisionHistory(string table)
        {
            // table is embedded into SQL object names; allow-list validate it
            table = Tools.ValidateIdentifier(table, "table");

            string dbserver = Tools.RequireSetting("dbserver");
            string database = Tools.RequireSetting("database");

            string json;
            string url;
            string url_count;
            int pagesize = 2000;

            string connString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);

            DataTable dataTable = new DataTable();
            DataTable opus = new DataTable();

            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = string.Format("Select * from [DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + table.ToUpper() + "] order by Results_id";
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(opus);
                    da.Dispose();
                }
            }

            using (SqlConnection connection = new SqlConnection(connString))
            {
                string query = string.Format("Select * from [DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + table.ToUpper() + "_RevisionHistory] order by Results_id";
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(dataTable);
                    da.Dispose();
                }
            }

            DataTable Revisions = new DataTable();

            Revisions.Columns.Add(new DataColumn
            {
                ColumnName = "ObjectUUID",
                DataType = typeof(string)
            });

            Revisions.Columns.Add(new DataColumn
            {
                ColumnName = "CreationDate",
                DataType = typeof(string)
            });

            Revisions.Columns.Add(new DataColumn
            {
                ColumnName = "Description",
                DataType = typeof(string)
            });

            foreach (DataRow dr in dataTable.Rows)
            {
                int CurrentResultID = Convert.ToInt32(dr["Results_Id"]);

                url_count = dr["_ref"].ToString() + "/Revisions?fetch=ObjectUUID&query=((((Description contains \"NOTES changed \") or (Description contains \"NOTES added \")) or (Description contains \"RAG added \")) or (Description contains \"RAG changed \"))&start=1&pagesize=1";
                url = dr["_ref"].ToString() + "/Revisions?query=((((Description contains \"NOTES changed \") or (Description contains \"NOTES added \")) or (Description contains \"RAG added \")) or (Description contains \"RAG changed \"))";

                string json_count = Tools.WebRequestWithToken(httpclient, url_count);

                JObject rss_count = JObject.Parse(json_count);
                int TotalResultCount = 0;
                if (rss_count.SelectToken("QueryResult.TotalResultCount") != null)
                {
                    TotalResultCount = (int)rss_count.SelectToken("QueryResult.TotalResultCount");
                }

                double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

                string varObjectUUID = opus.Rows[CurrentResultID]["_refObjectUUID"].ToString();

                int start = 1;

                for (int i = 1; i <= nbIteration; i++)
                {
                    url = url + "&start=" + start + "&pagesize=" + pagesize;
                    json = Tools.WebRequestWithToken(httpclient, url);
                    JObject rss = JObject.Parse(json);

                    string varCreationDate;
                    string varDescription;

                    // TODO(review): this loop iterates TotalResultCount per page instead of the
                    // page's actual result count. Preserved as-is (business logic); multi-page
                    // results may produce duplicate/empty rows. Fix requires business sign-off.
                    for (int j = 0; j < TotalResultCount; j++)
                    {
                        DataRow NewRow = Revisions.NewRow();
                        varCreationDate = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].CreationDate");
                        varDescription = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].Description");
                        if (varDescription != null)
                        {
                            NewRow["ObjectUUID"] = varObjectUUID;
                            NewRow["CreationDate"] = varCreationDate;
                            NewRow["Description"] = varDescription;
                            Revisions.Rows.Add(NewRow);
                        }
                    }
                    start = start + pagesize;
                }
            }

            // insert Revisions datatable into SQL table Revisions
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection))
                {
                    bulkcopy.BulkCopyTimeout = 0;
                    bulkcopy.DestinationTableName = string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + table.ToUpper() + "_Revisions]";
                    bulkcopy.WriteToServer(Revisions);
                }
                Revisions.Clear();
            }

            logger.Info("Opus Revisions table populated.");
        }

        public static void LoadFeatureException(string dbserver, string database, string table, DataTable ConfigTable)
        {
            // table is embedded into SQL object names; allow-list validate it
            table = Tools.ValidateIdentifier(table, "table");

            string connString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);
            string urlbase = Tools.ReadConfiguration(ConfigTable, "url", table);

            int AddDays = Tools.RequireSettingInt("AddDays");
            string LastUpdateDate = DateTime.Today.AddDays(AddDays * -1).ToString("yyyy-MM-dd");

            string json;
            string url = urlbase + "&query=((LastUpdateDate >= " + LastUpdateDate + ") AND (c_RequiredDeliveryDate != null))";
            int pagesize = 2000;

            string json_count = Tools.WebRequestWithToken(httpclient, url);
            int TotalResultCount = ReadTotalResultCount(json_count);

            double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

            DataTable DTexception = new DataTable();

            DTexception.Columns.Add(new DataColumn
            {
                ColumnName = "_rallyAPIMajor",
                DataType = typeof(string)
            });

            DTexception.Columns.Add(new DataColumn
            {
                ColumnName = "_rallyAPIMinor",
                DataType = typeof(string)
            });

            int start = 1;
            for (int i = 1; i <= nbIteration; i++)
            {
                url = urlbase + "&fetch=FormattedID,c_RequiredDeliveryDate&start=" + start + "&pagesize=" + pagesize + "&query=((LastUpdateDate >= " + LastUpdateDate + ") AND (c_RequiredDeliveryDate != null))";
                json = Tools.WebRequestWithToken(httpclient, url);
                JObject rss = JObject.Parse(json);

                string FormattedID;
                string c_RequiredDeliveryDate;

                for (int j = 0; j < TotalResultCount; j++)
                {
                    DataRow NewRow = DTexception.NewRow();
                    FormattedID = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].FormattedID");
                    c_RequiredDeliveryDate = (string)rss.SelectToken("QueryResult.Results[" + j.ToString() + "].c_RequiredDeliveryDate");
                    if (FormattedID != null)
                    {
                        NewRow["_rallyAPIMajor"] = FormattedID;
                        NewRow["_rallyAPIMinor"] = c_RequiredDeliveryDate;
                        DTexception.Rows.Add(NewRow);
                    }
                }
                start = start + pagesize;
            }

            // insert Revisions datatable into SQL table Revisions
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();

                // truncate ExpertiseDemands table
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = "[DMAS].[dbo].[st_truncate_table]";
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add(new SqlParameter("@Tablename", string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_FEATURE_ExpertiseDemands]", SqlSchema)));
                    cmd.ExecuteNonQuery();
                }

                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection))
                {
                    bulkcopy.DestinationTableName = string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_", SqlSchema) + table.ToUpper() + "_ExpertiseDemands]";
                    bulkcopy.WriteToServer(DTexception);
                }
                DTexception.Clear();

                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = string.Format("UPDATE [DMAS].[{0}].[TB_STG_CAAGILE_FEATURE] SET c_Businesswishdate = S._rallyAPIMinor FROM [DMAS].[{0}].[TB_STG_CAAGILE_FEATURE_ExpertiseDemands] S INNER JOIN [DMAS].[{0}].[TB_STG_CAAGILE_FEATURE] T ON T.FormattedID = S._rallyAPIMajor", SqlSchema);
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        // Parses TotalResultCount from a Rally count response. Throws InvalidOperationException
        // when the response does not contain a usable value (fail fast instead of crashing later).
        private static int ReadTotalResultCount(string json)
        {
            JObject parsed = JObject.Parse(json);
            JToken token = parsed.SelectToken("QueryResult.TotalResultCount");
            int count;
            if (token == null || !int.TryParse(token.ToString(), out count) || count < 0)
                throw new InvalidOperationException("Rally count response did not contain a valid QueryResult.TotalResultCount.");
            return count;
        }

        // LoadDataset is used to convert json in xml then load it into datasets
        // because CAAGILE has limitation to bring 2000 records at once, a loop is managed to get all needed records
        static void LoadDataset(DataSet dataset, DataTable ConfigTable, string table, string table_count, int pagesize)
        {
            string dbserver = Tools.RequireSetting("dbserver");
            string database = Tools.RequireSetting("database");
            int AddDays = Tools.RequireSettingInt("AddDays");

            string url_count = Tools.ReadConfiguration(ConfigTable, "url", table_count);
            string urlbase = Tools.ReadConfiguration(ConfigTable, "url", table);

            string LastUpdateDate = DateTime.Today.AddDays(AddDays * -1).ToString("yyyy-MM-dd");

            if (table == "userstory" || table == "feature")
            {
                url_count = url_count + "&fetch=ObjectUUID&start=1&pagesize=1&query=(LastUpdateDate >= " + LastUpdateDate + ")";
            }

            if (table == "iteration")
            {
                url_count = url_count + "&fetch=ObjectUUID&start=1&pagesize=1&query=(CreationDate >= " + LastUpdateDate + ")";
            }

            string json_count = Tools.WebRequestWithToken(httpclient, url_count);
            int TotalResultCount = ReadTotalResultCount(json_count);
            double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

            int start = 1;

            try
            {
                for (int i = 1; i <= nbIteration; i++)
                {
                    string url;
                    if (table == "userstory" || table == "feature")
                    {
                        url = urlbase + "&fetch=true&start=" + start + "&pagesize=" + pagesize + "&query=(LastUpdateDate >= " + LastUpdateDate + ")";
                    }
                    else
                    {
                        if (table == "iteration")
                        {
                            url = urlbase + "&fetch=true&start=" + start + "&pagesize=" + pagesize + "&query=(CreationDate >= " + LastUpdateDate + ")";
                        }
                        else
                        {
                            url = urlbase + "&start=" + start + "&pagesize=" + pagesize;
                        }
                    }

                    string json = Tools.WebRequestWithToken(httpclient, url);
                    try
                    {
                        XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);

                        foreach (DataTable dataTable in dataset.Tables)
                            dataTable.BeginLoadData();

                        using (StringReader xmlInput = new StringReader(doc.OuterXml))
                        using (XmlReader xmlReader = Tools.CreateSecureXmlReader(xmlInput))
                        {
                            dataset.ReadXml(xmlReader);
                        }

                        foreach (DataTable dataTable in dataset.Tables)
                            dataTable.EndLoadData();
                    }
                    catch (Exception e)
                    {
                        logger.Error(e, "table : {0} -- Error deserializing page starting at {1}", table, start);
                    }

                    // Bulkinsert SQL Tables

                    if (table == "risk")
                    {
                        DataTable tagsTable = dataset.Tables["_tagsNameArray"];
                        if (tagsTable != null)
                        {
                            var rows = tagsTable.Select("Tags_Id is null");
                            if (rows.Count() > 0)
                            {
                                foreach (var row in rows)
                                { row.Delete(); }
                                tagsTable.AcceptChanges();
                            }
                        }
                    }

                    Bulkinsertdynamic(dataset, ConfigTable, dbserver, database, table.ToString());
                    if (table.ToString() == "opus")
                    {
                        Bulkinsertdynamic(dataset, ConfigTable, dbserver, database, "epic");
                    }

                    dataset.Clear();

                    if (table == "feature")
                    {
                        DataTable resultsTable = dataset.Tables["Results"];
                        if (resultsTable != null)
                        {
                            DataColumn clarityColumn = resultsTable.Columns["c_zREMOVE2ClarityID"];
                            if (clarityColumn != null)
                                clarityColumn.ColumnName = "c_PERFTest";
                            DataColumn featureTypeColumn = resultsTable.Columns["c_zREMOVEBNPPFeatureType"];
                            if (featureTypeColumn != null)
                                featureTypeColumn.ColumnName = "c_WAVATest";
                        }
                    }

                    if (table == "opus")
                    {
                        DataTable resultsTable = dataset.Tables["Results"];
                        if (resultsTable != null)
                        {
                            DataColumn forecastColumn = resultsTable.Columns["c_PRMForecastDeliveryDate"];
                            if (forecastColumn != null)
                                forecastColumn.ColumnName = "c_PRMDeliveryDate";
                            DataColumn wishDateColumn = resultsTable.Columns["c_Businesswishdate"];
                            if (wishDateColumn != null)
                                wishDateColumn.ColumnName = "c_RequiredDeliveryDate";
                        }
                    }

                    start = start + pagesize;
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "table : {0} -- An error occurred in LoadDataset", table);
                throw;
            }
            finally
            {
                if (dataset.Tables.Contains("RevisionHistory") && table_count == "feature_count")
                {
                    // add feature creator into Revisionhistory
                    GetMilestones(ConfigTable, "feature", "feature");
                    UpdateDatatable();
                }
                if (dataset.Tables.Contains("RevisionHistory"))
                {
                    if (table_count == "opus_count")
                    {
                        GetRevisionHistory("opus");
                    }
                    else if (table_count == "initiative_count")
                    {
                        GetRevisionHistory("initiative");
                    }
                }
            }
        }

        // LoadDatasetExceptionUserstory handles the Userstory Parent exception where only few rows are recorded
        static void LoadDatasetExceptionUserstory(DataSet dataset, DataTable ConfigTable, string table, string table_count)
        {
            int AddDays = Tools.RequireSettingInt("AddDays");
            string LastUpdateDate = DateTime.Today.AddDays(AddDays * -1).ToString("yyyy-MM-dd");

            string url_count = "https://eu1.rallydev.com/slm/webservice/v2.0/hierarchicalrequirement?workspace=https://eu1.rallydev.com/slm/webservice/v2.0/workspace/65088890229&query=((Parent%20!=%20null) AND (LastUpdateDate >= " + LastUpdateDate + "))&fetch=TotalResultCount&start=1&pagesize=1";
            string urlbase = "https://eu1.rallydev.com/slm/webservice/v2.0/hierarchicalrequirement?workspace=https://eu1.rallydev.com/slm/webservice/v2.0/workspace/65088890229&query=((Parent%20!=%20null) AND (LastUpdateDate >= " + LastUpdateDate + "))&fetch=ObjectUUID,Parent";
            int pagesize = 2000;

            string json_count = Tools.WebRequestWithToken(httpclient, url_count);
            int TotalResultCount = ReadTotalResultCount(json_count);
            double nbIteration = Math.Ceiling((double)TotalResultCount / pagesize);

            int start = 1;

            try
            {
                for (int i = 1; i <= nbIteration; i++)
                {
                    string url = urlbase + "&start=" + start + "&pagesize=" + pagesize;

                    string json = Tools.WebRequestWithToken(httpclient, url);
                    try
                    {
                        XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);

                        foreach (DataTable dataTable in dataset.Tables)
                            dataTable.BeginLoadData();

                        using (StringReader xmlInput = new StringReader(doc.OuterXml))
                        using (XmlReader xmlReader = Tools.CreateSecureXmlReader(xmlInput))
                        {
                            dataset.ReadXml(xmlReader);
                        }

                        foreach (DataTable dataTable in dataset.Tables)
                            dataTable.EndLoadData();
                    }
                    catch (Exception e)
                    {
                        logger.Error(e, "Error deserializing page starting at {0}", start);
                    }

                    start = start + pagesize;
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "An error occurred in LoadDatasetExceptionUserstory");
                throw;
            }
            finally
            {
                // enrich Parent table

                string[] dest_Columns = { "_rallyAPIMajor", "_rallyAPIMinor", "_ref", "_refObjectUUID", "_refObjectName", "_type", "Results_Id" };
                DataTable dt_insert = new DataTable();

                DataTable resultsTable = dataset.Tables["Results"];
                if (resultsTable == null)
                {
                    logger.Warn("No 'Results' table available; Parent enrichment skipped.");
                }
                else
                {
                    foreach (DataTable dt in dataset.Tables)
                    {
                        if (dt.TableName.ToString() == "Parent")
                        {
                            int i = 0;
                            foreach (DataRow rw in dt.Rows)
                            {
                                if (i >= resultsTable.Rows.Count)
                                {
                                    logger.Warn("Fewer 'Results' rows than 'Parent' rows; remaining Parent rows keep their original _ref.");
                                    break;
                                }
                                rw["_ref"] = resultsTable.Rows[i].Field<string>("ObjectUUID") ?? string.Empty;
                                i++;
                            }
                            dt_insert = dt.Copy();
                        }
                    }
                }

                // bulkinsert Parent table

                string dbserver = Tools.RequireSetting("dbserver");
                string database = Tools.RequireSetting("database");
                string connString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    connection.Open();

                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection))
                    {
                        bulkcopy.DestinationTableName = string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_USERSTORY_Parent]", SqlSchema);

                        // Column Mapping
                        bulkcopy.ColumnMappings.Clear();
                        int length = dt_insert.Columns.Count;
                        for (int k = length - 1; k >= 0; k--)
                        {
                            try
                            {
                                if (dest_Columns.Contains(dt_insert.Columns[k].ColumnName.Trim()))
                                {
                                    bulkcopy.ColumnMappings.Add(dt_insert.Columns[k].ColumnName.Trim(), dt_insert.Columns[k].ColumnName.Trim());
                                }
                            }
                            catch (Exception e)
                            {
                                logger.Warn(e, "Error in columnmapping");
                            }
                        }

                        // Bulk Insert
                        bulkcopy.WriteToServer(dt_insert);
                    }
                    dt_insert.Clear();
                }

                logger.Info("UserStory Parent Table filled.");
            }
        }

        static void MakelinksforAffectedItemsforRisk(string dbserver, string database)
        {
            string connString = Tools.GetConnectionString(dbserver, database, useIntegratedSecurity);
            DataTable RefLinks = new DataTable();

            string query = string.Format("select * from [DMAS].[{0}].[TB_STG_CAAGILE_RISK_WorkItemsAffected]", SqlSchema) + " where [Count] > 0 order by [Results_Id]";
            using (SqlConnection connection = new SqlConnection(connString))
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(RefLinks);
                    da.Dispose();
                }
            }

            DataTable RefTargetToLoad = new DataTable();
            DataRow NewRow;

            RefTargetToLoad.Columns.Add(new DataColumn
            {
                ColumnName = "ObjectIDRisk",
                DataType = typeof(string)
            });

            RefTargetToLoad.Columns.Add(new DataColumn
            {
                ColumnName = "ObjectIDAffectedItem",
                DataType = typeof(string)
            });

            RefTargetToLoad.Columns.Add(new DataColumn
            {
                ColumnName = "Type",
                DataType = typeof(string)
            });

            foreach (DataRow row in RefLinks.Rows)
            {
                // Get the ref (URL) and invoke the Rest API
                string url = row["_ref"].ToString() + "?fetch=_refObjectUUID&start=1&pagesize=2000";
                string ObjectIDRisk = Regex.Match(url, @"[^\d](\d{11})[^\d]").Value;
                ObjectIDRisk = ObjectIDRisk.Replace("/", "");

                string json;
                try
                {
                    json = Tools.WebRequestWithToken(httpclient, url);
                }
                catch (InvalidOperationException ex)
                {
                    // a single unreadable reference must not stop the other references
                    logger.Error(ex, "Skipping risk affected-items URL after HTTP failure: {0}", Tools.SanitizeForLog(url));
                    continue;
                }

                try
                {
                    XmlDocument doc = (XmlDocument)JsonConvert.DeserializeXmlNode(json);
                    DataSet dt = new DataSet();

                    foreach (DataTable dataTable in dt.Tables)
                        dataTable.BeginLoadData();

                    using (StringReader xmlInput = new StringReader(doc.OuterXml))
                    using (XmlReader xmlReader = Tools.CreateSecureXmlReader(xmlInput))
                    {
                        dt.ReadXml(xmlReader);
                    }

                    foreach (DataTable dataTable in dt.Tables)
                        dataTable.EndLoadData();

                    if (dt.Tables["Results"] != null)
                    {
                        foreach (DataRow rwdt in dt.Tables["Results"].Rows)
                        {
                            NewRow = RefTargetToLoad.NewRow();
                            NewRow["ObjectIDRisk"] = ObjectIDRisk;
                            NewRow["ObjectIDAffectedItem"] = rwdt["_refObjectUUID"].ToString();
                            NewRow["Type"] = rwdt["_type"].ToString();
                            RefTargetToLoad.Rows.Add(NewRow);
                        }
                        dt.Clear();
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e, "An error occurred in load affected items - DeserializeXmlNode");
                }
            }

            RefLinks.Clear();

            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection))
                {
                    bulkcopy.DestinationTableName = string.Format("[DMAS].[{0}].[TB_STG_CAAGILE_RISK_LINKS]", SqlSchema);
                    bulkcopy.WriteToServer(RefTargetToLoad);
                }
            }
            RefTargetToLoad.Clear();

            logger.Info("Links for Table Risk have been created");
        }

        static void LoadDataFromAgile(DataSet SQLTargetSet, string dbserver, string database, DataTable ConfigTable, string table)
        {
            int BlockSize = Tools.RequireSettingInt("BlockSize");

            try
            {
                // table comes from configuration and flows into SQL object names; validate it
                table = Tools.ValidateIdentifier(table, "table");

                if (table == "userstory")
                {
                    Console.WriteLine("task load for {0} table has started", table.ToString());
                    logger.Info("task load for {0} table has started", table.ToString());
                    LoadDataset(SQLTargetSet, ConfigTable, table.ToString(), table.ToString() + "_count", BlockSize);
                    Console.WriteLine("task load for {0} table complete", table.ToString());
                    logger.Info("task load for {0} table complete", table.ToString());
                    SQLTargetSet.Dispose();
                    Console.WriteLine("Manage exceptions for {0} table", table.ToString());
                    logger.Info("Manage exceptions for {0} table", table.ToString());
                    DataSet ExceptionDataset = new DataSet();
                    LoadDatasetExceptionUserstory(ExceptionDataset, ConfigTable, table.ToString(), table.ToString() + "_count");
                    Console.WriteLine("task load for Parent UserStory done");
                    logger.Info("task load for Parent UserStory done");
                }
                else
                {
                    Console.WriteLine("task load for {0} table has started", table.ToString());
                    logger.Info("task load for {0} table has started", table.ToString());

                    LoadDataset(SQLTargetSet, ConfigTable, table.ToString(), table.ToString() + "_count", BlockSize);
                    Console.WriteLine("task load for {0} table complete", table.ToString());
                    logger.Info("task load for {0} table complete", table.ToString());

                    SQLTargetSet.Dispose();

                    if (table == "risk")
                    {
                        Console.WriteLine("Make links for affected Items by Risks started");
                        MakelinksforAffectedItemsforRisk(dbserver, database);
                        Console.WriteLine("Make links for affected Items by Risks completed");
                    }
                }
            }
            catch (Exception e)
            {
                // one failing table must not stop the other tables; it is reported
                // through the exit code at the end of the run
                logger.Error(e, "task load for {0} table has failed", table.ToString());
                Interlocked.Increment(ref _failedTables);
            }
        }

        // Main program to trigger the required methods....

        static int Main(string[] args)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            try
            {
                useIntegratedSecurity = Tools.RequireSettingBool("IntegratedSecurity");
                proxy = Tools.RequireSetting("proxy");
                // SqlSchema is embedded into every SQL object name; allow-list validate it once
                SqlSchema = Tools.ValidateIdentifier(Tools.RequireSetting("SqlSchema"), "SqlSchema");
                string dbserver = Tools.RequireSetting("dbserver");
                string database = Tools.RequireSetting("database");
                string configtable = Tools.RequireSetting("configtable");

                DataTable ConfigTabletoLoad = Tools.LoadConfiguration(dbserver, database, configtable);

                string[] listtabletotruncate = Tools.ReadListConfiguration(ConfigTabletoLoad, "dbstatement", "truncate");
                string[] listtabletoload = Tools.ReadListConfiguration(ConfigTabletoLoad, "load", "load");

                httpclient = Tools.WebAuthenticationWithToken(proxy);

                SQLstatement(dbserver, database, listtabletotruncate, true);

                Task[] tasks = new Task[listtabletoload.Length];
                int i = 0;
                foreach (var table in listtabletoload)
                {
                    DataSet SQLTargetSet = new DataSet();
                    tasks[i] = Task.Factory.StartNew(() => LoadDataFromAgile(SQLTargetSet, dbserver, database, ConfigTabletoLoad, table), TaskCreationOptions.LongRunning);
                    i++;
                }

                Task.WaitAll(tasks);

                //// Handle temporary exception for feature
                LoadFeatureException(dbserver, database, "feature", ConfigTabletoLoad);

                logger.Info("load into SQL Server Staging has been completed");

                if (_failedTables > 0)
                {
                    logger.Warn("{0} table load task(s) have failed during the run", _failedTables);
                    return 1;
                }

                return 0;
            }
            catch (Exception e)
            {
                logger.Error(e, "load into SQL Server Staging has failed");
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
