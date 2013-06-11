using Newtonsoft.Json.Linq;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Net.Http;
#if NETFX_CORE
using Windows.Security.Cryptography;
using Windows.Security.Cryptography.Core;
#else
using System.Security.Cryptography;
#endif
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.IO.Compression;

namespace SyncFoundation.Client
{
    public class HttpSyncTransport : ISyncTransport
    {
        private readonly Uri _remoteAddress;
        private readonly string _username;
        private readonly string _password;

        public HttpSyncTransport(Uri remoteAddress, string username, string password)
        {
            _remoteAddress = remoteAddress;
            _username = username;
            _password = password;
        }

        public async Task<JObject> TransportAsync(SyncEndpoint endpoint, JObject contents)
        {
            var request = new JObject(contents);
            AddCredentials(request);
            var content = new StringContent(request.ToString(),Encoding.UTF8,"application/json");
            var compressedContent = new CompressedContent(content, "gzip");

            Uri remoteEndpoint;
            switch (endpoint)
            {
                case SyncEndpoint.BeginSession:
                    remoteEndpoint = new Uri(_remoteAddress, "beginSession");
                    break;
                case SyncEndpoint.EndSession:
                    remoteEndpoint = new Uri(_remoteAddress, "endSession");
                    break;
                case SyncEndpoint.GetChanges:
                    remoteEndpoint = new Uri(_remoteAddress, "getChanges");
                    break;
                case SyncEndpoint.GetItemData:
                    remoteEndpoint = new Uri(_remoteAddress, "getItemData");
                    break;
                case SyncEndpoint.GetItemDataBatch:
                    remoteEndpoint = new Uri(_remoteAddress, "getItemDataBatch");
                    break;
                case SyncEndpoint.PutChanges:
                    remoteEndpoint = new Uri(_remoteAddress, "putChanges");
                    break;
                case SyncEndpoint.PutItemDataBatch:
                    remoteEndpoint = new Uri(_remoteAddress, "putItemDataBatch");
                    break;
                case SyncEndpoint.ApplyChanges:
                    remoteEndpoint = new Uri(_remoteAddress, "applyChanges");
                    break;
                default:
                    throw new Exception("Unknown endpoint");
            }

            var responseMessage = await Client.PostAsync(remoteEndpoint, compressedContent);
            var responseString = await responseMessage.Content.ReadAsStringAsync();

            if (!responseMessage.IsSuccessStatusCode)
                throw new Exception(String.Format("Remote call failed (HTTP Status Code {0}): {1}", responseMessage.StatusCode, responseString));

            var response = JObject.Parse(responseString);

            if (response["ErrorCode"] != null)
            {
                throw new Exception(String.Format("Remote call failed with error code {0} - {1}",response["ErrorCode"], response["ErrorMessage"]));
            }

            return response;
        }

        private HttpClient _client;
        private HttpClient Client
        {
            get
            {
                if (_client == null)
                {
                    _client = new HttpClient(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip })
                        {
                            Timeout = TimeSpan.FromMinutes(5)
                        };
                }
                return _client;
            }
        }

        private static byte[] GenerateNonce()
        {
#if NETFX_CORE
            byte[] nonce;
            CryptographicBuffer.CopyToByteArray(CryptographicBuffer.GenerateRandom(16), out nonce);
            return nonce;
#else
            var nonce = new byte[16];
            var rng = new RNGCryptoServiceProvider();
            rng.GetBytes(nonce);
            return nonce;
#endif
        }

        private static byte[] ComputeHash(byte[] source)
        {
#if NETFX_CORE
            var bufSource = CryptographicBuffer.CreateFromByteArray(source);
            // Create a HashAlgorithmProvider object.
            var hashProvider = HashAlgorithmProvider.OpenAlgorithm(HashAlgorithmNames.Sha1);

            // Hash the message.
            var buffHash = hashProvider.HashData(bufSource);
            byte[] digest;
            CryptographicBuffer.CopyToByteArray(buffHash, out digest);
            return digest;
#else
            return new SHA1Managed().ComputeHash(source);
#endif
        }

        private void AddCredentials(JObject request)
        {
            string now = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
            byte[] nonce = GenerateNonce();
            byte[] created = Encoding.UTF8.GetBytes(now);
            byte[] password = Encoding.UTF8.GetBytes(_password);
            byte[] digestSource = new byte[nonce.Length + created.Length + password.Length];

            for (int i = 0; i < nonce.Length; i++)
                digestSource[i] = nonce[i];
            for (int i = 0; i < created.Length; i++)
                digestSource[nonce.Length + i] = created[i];
            for (int i = 0; i < password.Length; i++)
                digestSource[created.Length + nonce.Length + i] = password[i];


            byte[] digestBytes = ComputeHash(digestSource);
            string digest = Convert.ToBase64String(digestBytes);

            request.Add("username", _username);
            request.Add("nonce", Convert.ToBase64String(nonce));
            request.Add("created", now);
            request.Add("digest", digest);
        }

    }

    internal class CompressedContent : HttpContent
    {
        private readonly HttpContent _originalContent;
        private readonly string _encodingType;

        public CompressedContent(HttpContent content, string encodingType)
        {
            if (content == null)
            {
                throw new ArgumentNullException("content");
            }

            if (encodingType == null)
            {
                throw new ArgumentNullException("encodingType");
            }

            _originalContent = content;
            _encodingType = encodingType.ToLowerInvariant();

            if (_encodingType != "gzip" && _encodingType != "deflate")
            {
                throw new InvalidOperationException(string.Format("Encoding '{0}' is not supported. Only supports gzip or deflate encoding.", _encodingType));
            }

            // copy the headers from the original content
            foreach (KeyValuePair<string, IEnumerable<string>> header in _originalContent.Headers)
            {
                Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            Headers.ContentEncoding.Add(encodingType);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;

            return false;
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            Stream compressedStream = null;

            if (_encodingType == "gzip")
            {
                compressedStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);
            }
            else if (_encodingType == "deflate")
            {
                compressedStream = new DeflateStream(stream, CompressionMode.Compress, leaveOpen: true);
            }

            return _originalContent.CopyToAsync(compressedStream).ContinueWith(tsk =>
            {
                if (compressedStream != null)
                {
                    compressedStream.Dispose();
                }
            });
        }
    }

}
