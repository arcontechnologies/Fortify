using System;
using System.Configuration;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using SQLAccount_ClassLibrary;

namespace RecycleBin
{
    // Helper code for this application. This file is intentionally self-contained:
    // LoadStaging, Links and MCleaning carry equivalent copies by design (the five
    // pipeline apps stay fully independent). When fixing a method here, port the
    // same fix to the other copies — see the HELPERS-SYNC policy.
    class Helpers
    {
        private const string CredentialServer = "LIS0116950.warp.net.intra,1440";
        private const string CredentialName = "RALLY_TOKEN";

        private static readonly object HttpThrottleLock = new object();
        private static SemaphoreSlim _httpThrottle;
        private static readonly Random JitterRandom = new Random();

        // Returns a required app setting or fails fast with a clear message.
        public static string RequireSetting(string key)
        {
            string value = ConfigurationManager.AppSettings[key];
            if (string.IsNullOrWhiteSpace(value))
                throw new ConfigurationErrorsException(string.Format("Missing required app setting '{0}'.", key));
            return value;
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

        public static string GetConnectionString(string server, string database, bool useIntegratedSecurity)
        {
            string port = ConfigurationManager.AppSettings["SqlServerPort"];
            string dataSource = !string.IsNullOrEmpty(port) ? server + "," + port : server;

            if (useIntegratedSecurity)
            {
                return string.Format("Data Source={0};Initial Catalog={1};Integrated Security=True;Connection Timeout=30; MultiSubnetFailover=True;", dataSource, database);
            }

            string user = ConfigurationManager.AppSettings["SqlServerUserName"] ?? "";
            string password = ConfigurationManager.AppSettings["SqlServerPassword"] ?? "";
            return string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3};Connection Timeout=30;", dataSource, database, user, password);
        }

        private static (string UserName, string Password) GetSqlCredentials(string pwServer, string serverName)
        {
            var result = SQL_Account.SQL_GetPW(pwServer, serverName);
            return (result.UserName, result.PW);
        }

        private static string GetRallyApiToken()
        {
            var credentials = GetSqlCredentials(CredentialServer, CredentialName);
            if (string.IsNullOrWhiteSpace(credentials.Password))
                throw new InvalidOperationException("Rally API token is empty. Check the credential store entry '" + CredentialName + "'.");
            return credentials.Password.Trim();
        }

        public static HttpClient WebAuthenticationWithToken(string proxy)
        {
            HttpClientHandler handler = new HttpClientHandler
            {
                Proxy = new WebProxy(proxy, false),
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
                            RecycleBin.logger.Warn("Proxy/HTML response on attempt {0}/{1} (status {2})", attempt, maxRetries, statusCode);
                        }
                        else
                        {
                            lastBodySnippet = TruncateForLog(body, 200);
                            lastStatusCode = statusCode;
                            RecycleBin.logger.Warn("HTTP {0} ({1}) on attempt {2}/{3}", statusCode, message.ReasonPhrase, attempt, maxRetries);

                            if (!IsTransientHttpStatus(statusCode))
                            {
                                string fatalMsg = string.Format("Non-retryable HTTP {0} for URL: {1}", statusCode, url);
                                RecycleBin.logger.Error(fatalMsg);
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
                    Console.WriteLine("Http Response error (attempt {0}/{1}): {2}", attempt, maxRetries, e.Message);
                    RecycleBin.logger.Warn(e, "HTTP request failed on attempt {0}/{1}", attempt, maxRetries);
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
                url);
            RecycleBin.logger.Error(exhaustedMsg);
            if (lastException != null)
                RecycleBin.logger.Error(lastException, "Last HTTP exception");
            if (!string.IsNullOrEmpty(lastBodySnippet))
                RecycleBin.logger.Error("Last response snippet: {0}", lastBodySnippet);

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
            lock (JitterRandom)
            {
                return capped + JitterRandom.Next(0, Math.Max(1, baseDelayMs / 2));
            }
        }

        private static void WaitBeforeRetry(int attempt, int baseDelayMs)
        {
            Thread.Sleep(CalculateBackoffMs(attempt, baseDelayMs));
        }

        private static string TruncateForLog(string text, int maxLength)
        {
            if (string.IsNullOrEmpty(text) || text.Length <= maxLength)
                return text;
            return text.Substring(0, maxLength) + "...";
        }
    }
}
