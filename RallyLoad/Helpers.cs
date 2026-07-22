using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using SQLAccount_ClassLibrary;

namespace LoadStaging
{
    // Helper code for this application. This file is intentionally self-contained:
    // the other pipeline apps carry equivalent copies by design (the five pipeline
    // apps stay fully independent). When fixing a method here, port the same fix
    // to the other copies — see the HELPERS-SYNC policy.
    class Tools
    {
        private const string CredentialName = "RALLY_TOKEN";

        // Hosts that outbound requests (which carry the Rally API token) are allowed to
        // target by default. Override with the comma-separated 'allowedUrlHosts' app setting.
        private static readonly string[] DefaultAllowedUrlHosts = { "eu1.rallydev.com", "rally1.rallydev.com" };

        // Allow-list for SQL identifiers (schema/table/column name segments) before they
        // are embedded into command text or destination table names.
        private static readonly Regex SafeIdentifierRegex = new Regex("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

        // Hardened XML settings: no DTD processing, no external entity resolution (XXE / XML bombs).
        private static readonly XmlReaderSettings SecureXmlSettings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            MaxCharactersFromEntities = 0
        };

        private static readonly object HttpThrottleLock = new object();
        private static SemaphoreSlim _httpThrottle;

        // Per-run cache of staging table column lists (keyed by table name).
        private static readonly ConcurrentDictionary<string, DataTable> SchemaCache = new ConcurrentDictionary<string, DataTable>();

        // Returns a required app setting or fails fast with a clear message.
        public static string RequireSetting(string key)
        {
            string value = ConfigurationManager.AppSettings[key];
            if (string.IsNullOrWhiteSpace(value))
                throw new ConfigurationErrorsException(string.Format("Missing required app setting '{0}'.", key));
            return value;
        }

        // Returns a required boolean app setting or fails fast with a clear message.
        public static bool RequireSettingBool(string key)
        {
            string value = RequireSetting(key);
            bool parsed;
            if (!bool.TryParse(value, out parsed))
                throw new ConfigurationErrorsException(string.Format("App setting '{0}' must be 'true' or 'false'.", key));
            return parsed;
        }

        // Returns a required integer app setting or fails fast with a clear message.
        public static int RequireSettingInt(string key)
        {
            string value = RequireSetting(key);
            int parsed;
            if (!int.TryParse(value, out parsed))
                throw new ConfigurationErrorsException(string.Format("App setting '{0}' must be a valid integer.", key));
            return parsed;
        }

        private static int GetAppSettingInt(string key, int defaultValue)
        {
            string value = ConfigurationManager.AppSettings[key];
            int parsed;
            if (!string.IsNullOrEmpty(value) && int.TryParse(value, out parsed))
                return parsed;
            return defaultValue;
        }

        private static SemaphoreSlim GetHttpThrottle()
        {
            if (_httpThrottle == null)
            {
                lock (HttpThrottleLock)
                {
                    if (_httpThrottle == null)
                    {
                        int maxParallel = GetAppSettingInt("maxParallelHttp", 3);
                        _httpThrottle = new SemaphoreSlim(maxParallel > 0 ? maxParallel : 3);
                    }
                }
            }
            return _httpThrottle;
        }

        // Validates a (possibly qualified) SQL identifier against a strict allow-list
        // pattern. Qualified names such as "schema.table" or "database.schema.table" are
        // accepted when every dot-separated segment is a valid identifier. Throws on any
        // other input so unsafe values can never reach SQL.
        public static string ValidateIdentifier(string identifier, string parameterName)
        {
            if (!string.IsNullOrEmpty(identifier))
            {
                string[] segments = identifier.Split('.');
                if (segments.Length <= 3)
                {
                    bool allSegmentsValid = true;
                    foreach (string segment in segments)
                    {
                        if (!SafeIdentifierRegex.IsMatch(segment))
                        {
                            allSegmentsValid = false;
                            break;
                        }
                    }
                    if (allSegmentsValid)
                        return identifier;
                }
            }

            throw new ArgumentException(
                string.Format("Invalid SQL identifier '{0}': only letters, digits, underscores and dots (as name separators) are allowed.",
                    TruncateForLog(SanitizeForLog(identifier ?? "<null>"), 64)),
                parameterName);
        }

        // Strips CR/LF/TAB so remote-controlled text cannot forge log entries.
        public static string SanitizeForLog(string text)
        {
            if (string.IsNullOrEmpty(text))
                return text;
            return text.Replace("\r", " ").Replace("\n", " ").Replace("\t", " ");
        }

        // Escapes single quotes so a value can be safely embedded in a DataTable.Select filter.
        private static string EscapeSelectValue(string value)
        {
            return value == null ? string.Empty : value.Replace("'", "''");
        }

        // Creates an XmlReader hardened against XXE and entity-expansion (XML bomb) attacks.
        public static XmlReader CreateSecureXmlReader(TextReader input)
        {
            if (input == null)
                throw new ArgumentNullException("input");
            return XmlReader.Create(input, SecureXmlSettings);
        }

        // Ensures outbound requests (which carry the API token) only target allow-listed
        // HTTPS hosts, preventing token leakage through poisoned configuration or data.
        public static void ValidateOutboundUrl(string url)
        {
            Uri uri;
            if (!Uri.TryCreate(url, UriKind.Absolute, out uri) || uri.Scheme != Uri.UriSchemeHttps)
                throw new InvalidOperationException("Refusing outbound request: URL is not an absolute HTTPS address.");

            string configured = ConfigurationManager.AppSettings["allowedUrlHosts"];
            string[] allowedHosts = !string.IsNullOrWhiteSpace(configured)
                ? configured.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                : DefaultAllowedUrlHosts;

            bool isAllowed = false;
            foreach (string host in allowedHosts)
            {
                if (string.Equals(host.Trim(), uri.Host, StringComparison.OrdinalIgnoreCase))
                {
                    isAllowed = true;
                    break;
                }
            }

            if (!isAllowed)
                throw new InvalidOperationException("Refusing outbound request to non-allow-listed host '" + uri.Host + "'.");
        }

        // Cryptographically secure random for retry jitter (replaces System.Random).
        private static int GetSecureRandomInt(int maxExclusive)
        {
            using (RandomNumberGenerator rng = RandomNumberGenerator.Create())
            {
                byte[] buffer = new byte[4];
                rng.GetBytes(buffer);
                return (int)(BitConverter.ToUInt32(buffer, 0) % (uint)maxExclusive);
            }
        }

        public static string GetConnectionString(string server, string database, bool useIntegratedSecurity)
        {
            if (string.IsNullOrWhiteSpace(server))
                throw new ArgumentException("A database server is required.", "server");
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("A database name is required.", "database");

            string port = ConfigurationManager.AppSettings["SqlServerPort"];
            string dataSource = !string.IsNullOrEmpty(port) ? server + "," + port : server;

            // SqlConnectionStringBuilder neutralizes special characters in the configured
            // values (connection-string injection) instead of raw string concatenation.
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = dataSource,
                InitialCatalog = database,
                ConnectTimeout = 30
            };

            if (useIntegratedSecurity)
            {
                builder.IntegratedSecurity = true;
                builder.MultiSubnetFailover = true;
            }
            else
            {
                string user = ConfigurationManager.AppSettings["SqlServerUserName"];
                string password = ConfigurationManager.AppSettings["SqlServerPassword"];
                if (string.IsNullOrEmpty(user) || string.IsNullOrEmpty(password))
                {
                    throw new ConfigurationErrorsException(
                        "SQL authentication is enabled but 'SqlServerUserName' and/or 'SqlServerPassword' are missing or empty in configuration.");
                }
                builder.UserID = user;
                builder.Password = password;
            }

            return builder.ConnectionString;
        }

        // Returns the column list of a SQL table (cached for the duration of the run).
        public static DataTable GetSQLTable(string dbserver, string database, string tableName)
        {
            return SchemaCache.GetOrAdd(tableName, t => LoadSQLTable(dbserver, database, t));
        }

        private static DataTable LoadSQLTable(string dbserver, string database, string tableName)
        {
            DataTable SQLTable = new DataTable();
            string connString = GetConnectionString(dbserver, database, LoadStaging.useIntegratedSecurity);
            string query = "SELECT COLUMN_NAME FROM DMAS.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName";

            using (SqlConnection connection = new SqlConnection(connString))
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    cmd.Parameters.Add(new SqlParameter("@TableName", tableName));
                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(SQLTable);
                    da.Dispose();
                }
            }

            return SQLTable;
        }

        public static DataTable GetAGTable(DataTable datatable)
        {
            DataTable columntable = new DataTable();
            string[] columnNames = (from dc in datatable.Columns.Cast<DataColumn>()
                                    select dc.ColumnName).ToArray();

            columntable.Columns.Add(new DataColumn
            {
                ColumnName = "COLUMN_NAME",
                DataType = typeof(string)
            });

            foreach (var str in columnNames)
            {
                columntable.Rows.Add(str);
            }
            return columntable;
        }

        public static Tuple<bool, string> GetMappedColumn(string[] MappedConfiguration, string AGColumnName)
        {
            if (MappedConfiguration == null)
                return Tuple.Create(false, "");

            for (int i = 0; i < MappedConfiguration.Length; i++)
            {
                string entry = MappedConfiguration[i];
                if (string.IsNullOrEmpty(entry))
                    continue;

                int firstColon = entry.IndexOf(':');
                if (firstColon < 0)
                    continue;

                string MappedAGColumn = entry.Substring(entry.LastIndexOf(':') + 1);
                if (MappedAGColumn == AGColumnName)
                {
                    string MappedSQLColumn = entry.Substring(0, firstColon);
                    return Tuple.Create(true, MappedSQLColumn);
                }
            }
            return Tuple.Create(false, "");
        }

        public static DataTable CompareRows(DataTable SQLtable, DataTable AGtable, DataTable Resulttable, string[] listmappedcolumn)
        {
            Tuple<bool, string> MappedColumn;

            for (int i = AGtable.Rows.Count - 1; i >= 0; i--)
            {
                bool isfound = false;
                foreach (DataRow SQLrow in SQLtable.Rows)
                {
                    var SQLarray = SQLrow.ItemArray;
                    string CurrentColumnValue = AGtable.Rows[i]["COLUMN_NAME"].ToString();

                    if (SQLarray.Contains(CurrentColumnValue))
                    {
                        isfound = true;
                        break;
                    }
                }

                if (!isfound)
                {
                    // case where column should be mapped to a new column
                    MappedColumn = GetMappedColumn(listmappedcolumn, AGtable.Rows[i]["COLUMN_NAME"].ToString());

                    if (MappedColumn.Item1 == true)
                    {
                        Resulttable.Columns[i].ColumnName = MappedColumn.Item2.ToString();
                    }
                    else
                    {
                        // case where column should be deleted
                        Resulttable.Columns.Remove(Resulttable.Columns[i]);
                    }
                }
            }
            return Resulttable;
        }

        // method to load configuration data from Configtable declared in app.config
        public static DataTable LoadConfiguration(string dbserver, string database, string configtable)
        {
            DataTable datatable = new DataTable();
            string connString = GetConnectionString(dbserver, database, LoadStaging.useIntegratedSecurity);
            // table names cannot be parameterized; allow-list validated before concatenation
            string query = "select * from " + ValidateIdentifier(configtable, "configtable");

            using (SqlConnection connection = new SqlConnection(connString))
            {
                using (SqlCommand cmd = new SqlCommand(query, connection))
                {
                    connection.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(datatable);
                    da.Dispose();
                }
            }

            return datatable;
        }

        // given a Category and a Key, this function returns the related value.
        public static string ReadConfiguration(DataTable datatable, string category, string key)
        {
            if (datatable == null)
                throw new ArgumentNullException("datatable");

            string expression = "Category Like '" + EscapeSelectValue(category) + "' and Key Like '" + EscapeSelectValue(key) + "'";
            DataRow[] value = datatable.Select(expression);

            if (value.Length == 0)
            {
                throw new ConfigurationErrorsException(string.Format(
                    "Configuration entry not found (category '{0}', key '{1}').",
                    SanitizeForLog(category), SanitizeForLog(key)));
            }

            return value[0][3].ToString();
        }

        // given a Category and a Key, this function returns the list of related value but only for those who are enabled.
        public static string[] ReadListConfiguration(DataTable datatable, string category, string key)
        {
            if (datatable == null)
                throw new ArgumentNullException("datatable");

            DataRow[] value;

            value = datatable.Select("Category Like '" + EscapeSelectValue(category) + "' and Key Like '" + EscapeSelectValue(key) + "' and Enabled = 1");

            string[] result = new string[value.Length];
            int i = 0;

            foreach (var dr in value)
            {
                result[i] = value[i][3].ToString();
                i++;
            }
            return result;
        }

        private static (string UserName, string Password) GetSqlCredentials(string pwServer, string serverName)
        {
            var result = SQL_Account.SQL_GetPW(pwServer, serverName);
            return (result.UserName, result.PW);
        }

        private static string GetRallyApiToken()
        {
            var credentials = GetSqlCredentials(RequireSetting("credentialServer"), CredentialName);
            if (string.IsNullOrWhiteSpace(credentials.Password))
                throw new InvalidOperationException("Rally API token is empty. Check the credential store entry '" + CredentialName + "'.");
            return credentials.Password.Trim();
        }

        public static HttpClient WebAuthenticationWithToken(string proxy)
        {
            Uri proxyUri;
            if (!Uri.TryCreate(proxy, UriKind.Absolute, out proxyUri))
                throw new ConfigurationErrorsException("The 'proxy' app setting is not a valid absolute URI.");

            HttpClientHandler handler = new HttpClientHandler
            {
                Proxy = new WebProxy(proxyUri, false),
                UseProxy = true,
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };
            HttpClient confClient = new HttpClient(handler);

            int timeoutSeconds = GetAppSettingInt("httpTimeoutSeconds", 180);
            confClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds > 0 ? timeoutSeconds : 180);
            confClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + GetRallyApiToken());
            confClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            confClient.DefaultRequestHeaders.Add("ZSESSIONID", "");

            return confClient;
        }

        // GET with bounded retries: exponential backoff + jitter, transient-status
        // detection and a proxy/HTML-response guard. Throws InvalidOperationException
        // when retries are exhausted or the status code is not retryable.
        public static string WebRequestWithToken(HttpClient confClient, string url)
        {
            // the request carries the API token; only allow-listed HTTPS hosts are allowed
            ValidateOutboundUrl(url);
            string safeUrl = SanitizeForLog(url);

            int maxRetries = GetAppSettingInt("nbRetry", 8);
            int baseDelayMs = GetAppSettingInt("retryDelayMs", 2000);
            SemaphoreSlim throttle = GetHttpThrottle();

            Exception lastException = null;
            string lastBodySnippet = null;
            int? lastStatusCode = null;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                throttle.Wait();
                try
                {
                    using (HttpResponseMessage message = confClient.GetAsync(url).ConfigureAwait(false).GetAwaiter().GetResult())
                    {
                        string body = message.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                        int statusCode = (int)message.StatusCode;

                        if (message.IsSuccessStatusCode)
                        {
                            if (!IsProxyOrHtmlResponse(body))
                                return body;

                            lastBodySnippet = TruncateForLog(body, 200);
                            lastStatusCode = statusCode;
                            LoadStaging.logger.Warn("Proxy/HTML response on attempt {0}/{1} (status {2})", attempt, maxRetries, statusCode);
                        }
                        else
                        {
                            lastBodySnippet = TruncateForLog(body, 200);
                            lastStatusCode = statusCode;
                            LoadStaging.logger.Warn("HTTP {0} ({1}) on attempt {2}/{3}", statusCode, SanitizeForLog(message.ReasonPhrase), attempt, maxRetries);

                            if (!IsTransientHttpStatus(statusCode))
                            {
                                string fatalMsg = string.Format("Non-retryable HTTP {0} for URL: {1}", statusCode, safeUrl);
                                LoadStaging.logger.Error(fatalMsg);
                                throw new InvalidOperationException(fatalMsg);
                            }
                        }
                    }
                }
                catch (InvalidOperationException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    lastException = e;
                    LoadStaging.logger.Warn(e, "HTTP request failed on attempt {0}/{1}", attempt, maxRetries);
                }
                finally
                {
                    throttle.Release();
                }

                if (attempt < maxRetries)
                    WaitBeforeRetry(attempt, baseDelayMs);
            }

            string exhaustedMsg = string.Format(
                "HTTP retries exhausted ({0} attempts). Last status: {1}. URL: {2}",
                maxRetries,
                lastStatusCode.HasValue ? lastStatusCode.Value.ToString() : "n/a",
                safeUrl);
            LoadStaging.logger.Error(exhaustedMsg);
            if (lastException != null)
                LoadStaging.logger.Error(lastException, "Last HTTP exception");
            if (!string.IsNullOrEmpty(lastBodySnippet))
                LoadStaging.logger.Error("Last response snippet: {0}", lastBodySnippet);

            throw new InvalidOperationException(exhaustedMsg);
        }

        private static bool IsProxyOrHtmlResponse(string body)
        {
            if (string.IsNullOrWhiteSpace(body))
                return true;
            return body.TrimStart().StartsWith("<", StringComparison.Ordinal);
        }

        private static bool IsTransientHttpStatus(int statusCode)
        {
            return statusCode == 408 || statusCode == 429
                || statusCode == 502 || statusCode == 503 || statusCode == 504;
        }

        private static int CalculateBackoffMs(int attempt, int baseDelayMs)
        {
            int exponential = baseDelayMs * (int)Math.Pow(2, Math.Min(attempt - 1, 6));
            int capped = Math.Min(exponential, 60000);
            return capped + GetSecureRandomInt(Math.Max(1, baseDelayMs / 2));
        }

        private static void WaitBeforeRetry(int attempt, int baseDelayMs)
        {
            Thread.Sleep(CalculateBackoffMs(attempt, baseDelayMs));
        }

        private static string TruncateForLog(string text, int maxLength)
        {
            if (string.IsNullOrEmpty(text))
                return text;
            string sanitized = SanitizeForLog(text);
            if (sanitized.Length <= maxLength)
                return sanitized;
            return sanitized.Substring(0, maxLength) + "...";
        }
    }
}
