using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Alife.Framework;
using Alife.Function.FunctionCaller;
using Microsoft.Extensions.Logging;

namespace YuYang.QQTools;

public class QQToolsConfig
{
    [DisplayName("OneBot WebSocket地址")]
    [Description("连接OneBot v11的WebSocket地址")]
    public string Url { get; set; } = "ws://127.0.0.1:3001";

    [DisplayName("认证Token")]
    [Description("可选，如果OneBot配置了Token需要填写")]
    public string Token { get; set; } = "";

    [DisplayName("Cookie刷新间隔(分钟)")]
    [Description("定期刷新QQ空间Cookie的间隔时间")]
    public int CookieRefreshMinutes { get; set; } = 120;
}

[Module("QQ工具箱", "提供QQ点赞、戳一戳、撤回消息、空间说说等功能", null, null, 0, "幼央")]
public class QQToolsModule : ChatBehaviour, IConfigurable<QQToolsConfig>, IConfigurable
{
    private readonly XmlFunctionCaller _functionCaller;
    private readonly ILogger<QQToolsModule> _logger;
    private readonly Interactor<QQToolsModule> _interactor;

    private ClientWebSocket? _ws;
    private long _botId;
    private long _nextEchoId = 1L;
    private long _lastSentMessageId;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonElement>> _pendingActions = new();
    private string? _cachedCookies;
    private DateTime _cookieFetchTime = DateTime.MinValue;
    private readonly SemaphoreSlim _cookieLock = new(1, 1);

    private static readonly HttpClient Http = new(new HttpClientHandler
    {
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    })
    {
        Timeout = TimeSpan.FromSeconds(20)
    };

    public QQToolsConfig Configuration { get; set; }

    public QQToolsModule(XmlFunctionCaller functionCaller, ILogger<QQToolsModule> logger, Interactor<QQToolsModule> interactor)
    {
        _functionCaller = functionCaller;
        _logger = logger;
        _interactor = interactor;
    }

    protected override async Task OnAwake()
    {
        _logger.LogInformation("QQToolsModule 已启动");
        await ConnectAsync();

        var handler = new XmlHandler(this)
        {
            Description = "QQ工具箱：点赞、戳一戳、撤回消息、空间说说等",
            Explanation = "提供QQ点赞、戳一戳、撤回消息与QQ空间互动功能"
        };
        _functionCaller.RegisterHandler(handler, DocumentMode.Implicit, DestroyCancellationToken);

        _interactor.Prompt("你具备「QQ工具箱」能力，提供以下功能：\r\n1. SendLike：给指定QQ用户点赞（1-10次）\r\n2. PokeGroupMember：在群聊中戳一戳指定成员\r\n3. DeleteMessage：撤回一条消息（需提供消息ID）\r\n4. PublishMood：发表QQ空间说说（纯文字）\r\n5. LikeMood：点赞QQ空间说说（需作者QQ号和说说tid）\r\n6. UnlikeMood：取消点赞QQ空间说说\r\n7. GetMoodList：获取QQ空间最新说说列表（默认自己的，可指定他人QQ号）\r\n\r\n使用场景：\r\n- 用户说\"点赞\"、\"戳一戳\"时使用互动功能\r\n- 用户说\"撤回\"、\"撤销\"时使用DeleteMessage\r\n- 用户说\"发说说\"、\"发空间\"时使用PublishMood\r\n- 用户说\"点赞说说\"、\"取消赞\"时使用LikeMood/UnlikeMood\r\n- 用户说\"看看说说\"、\"空间列表\"时使用GetMoodList");
    }

    protected override async Task OnDestroy()
    {
        try
        {
            _ws?.Dispose();
        }
        catch
        {
        }
        await base.OnDestroy();
    }

    private async Task ConnectAsync()
    {
        try
        {
            _ws?.Dispose();
            _ws = new ClientWebSocket();
            if (!string.IsNullOrEmpty(Configuration.Token))
            {
                _ws.Options.SetRequestHeader("Authorization", "Bearer " + Configuration.Token);
            }
            await _ws.ConnectAsync(new Uri(Configuration.Url), CancellationToken.None);
            _ = ReceiveLoopAsync();
            _logger.LogInformation("QQTools已连接到OneBot: {Url}", Configuration.Url);
            await CallActionAsync<JsonElement>("get_login_info");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "QQTools连接OneBot失败");
            await Task.Delay(5000);
            await ConnectAsync();
        }
    }

    private async Task ReceiveLoopAsync()
    {
        if (_ws == null) return;
        byte[] buffer = new byte[65536];
        try
        {
            while (_ws.State == WebSocketState.Open)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    ms.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                using var doc = JsonDocument.Parse(Encoding.UTF8.GetString(ms.ToArray()));
                JsonElement root = doc.RootElement;

                if (root.TryGetProperty("post_type", out var postTypeElement))
                {
                    string postType = postTypeElement.GetString() ?? "";
                    if (root.TryGetProperty("self_id", out var selfIdElement))
                    {
                        _botId = selfIdElement.GetInt64();
                    }
                    _logger.LogInformation("收到OneBot事件: type={Type}", postType);

                    if ((postType == "message" || postType == "message_sent") &&
                        root.TryGetProperty("message_id", out var msgIdElement))
                    {
                        long msgId = msgIdElement.GetInt64();
                        string rawMsg = root.TryGetProperty("raw_message", out var rawMsgElement)
                            ? rawMsgElement.GetString() ?? ""
                            : "";
                        bool isSelfSent = postType == "message_sent" ||
                            (root.TryGetProperty("user_id", out var userIdElement) && userIdElement.GetInt64() == _botId);
                        if (isSelfSent)
                        {
                            _lastSentMessageId = msgId;
                        }
                        _logger.LogInformation("QQTools捕获消息: id={MsgId} self={IsSelf} content={Content}", msgId, isSelfSent, rawMsg);
                    }

                    if (postType == "request")
                    {
                        _ = Task.Run(() => HandleRequestAsync(root.Clone()));
                    }
                    continue;
                }

                if (root.TryGetProperty("echo", out var echoElement) && echoElement.ValueKind == JsonValueKind.String)
                {
                    string echo = echoElement.GetString() ?? "";
                    if (!string.IsNullOrEmpty(echo) && _pendingActions.TryRemove(echo, out var tcs))
                    {
                        tcs.TrySetResult(root.Clone());
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "接收循环异常");
        }
        finally
        {
            await Task.Delay(3000);
            await ConnectAsync();
        }
    }

    private async Task HandleRequestAsync(JsonElement req)
    {
        try
        {
            string requestType = req.TryGetProperty("request_type", out var rtElement)
                ? rtElement.GetString() ?? "" : "";
            if (requestType == "friend")
            {
                string flag = req.TryGetProperty("flag", out var flagElement)
                    ? flagElement.GetString() ?? "" : "";
                long userId = req.TryGetProperty("user_id", out var uidElement)
                    ? uidElement.GetInt64() : 0;
                string comment = req.TryGetProperty("comment", out var commentElement)
                    ? commentElement.GetString() ?? "" : "";
                _logger.LogInformation("收到好友申请: QQ={UserId}, 验证信息={Comment}", userId, comment);

                await CallActionAsync<JsonElement>("set_friend_add_request", new
                {
                    flag = flag,
                    approve = true,
                    remark = "猫娘幼央"
                });
                _logger.LogInformation("已自动同意好友申请: QQ={UserId}, 验证信息={Comment}", userId, comment);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "处理请求事件失败");
        }
    }

    private async Task<T?> CallActionAsync<T>(string action, object? @params = null)
    {
        if (_ws == null || _ws.State != WebSocketState.Open)
        {
            throw new InvalidOperationException("OneBot未连接");
        }

        string echo = $"qqtools_{_nextEchoId++}";
        var tcs = new TaskCompletionSource<JsonElement>();
        _pendingActions.TryAdd(echo, tcs);

        string payload = JsonSerializer.Serialize(new { action, @params, echo });
        await _ws.SendAsync(
            new ArraySegment<byte>(Encoding.UTF8.GetBytes(payload)),
            WebSocketMessageType.Text, true, CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var reg = cts.Token.Register(() => tcs.TrySetCanceled());
        try
        {
            JsonElement response = await tcs.Task;
            if (response.TryGetProperty("retcode", out var retcodeElement) && retcodeElement.GetInt32() != 0)
            {
                string errorMsg = response.TryGetProperty("message", out var msgElement)
                    ? msgElement.GetString() ?? "" : "";
                throw new Exception($"调用失败: {errorMsg}");
            }
            return response.TryGetProperty("data", out var dataElement)
                ? dataElement.Deserialize<T>()
                : default;
        }
        catch (TaskCanceledException)
        {
            _pendingActions.TryRemove(echo, out _);
            throw new TimeoutException("调用超时");
        }
    }

    #region ===== QQ点赞/戳一戳/消息 =====

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("给指定QQ用户点赞，返回点赞结果（1-10次）")]
    public async Task<string> SendLike(long userId, int times = 1)
    {
        try
        {
            times = Math.Clamp(times, 1, 10);
            await CallActionAsync<JsonElement>("send_like", new { user_id = userId, times });
            _interactor.Poke($"成功给 {userId} 点了{times}次赞喵！");
            return $"成功给 {userId} 点了{times}次赞喵！";
        }
        catch (Exception ex)
        {
            _interactor.Poke("点赞失败喵: " + ex.Message);
            return "点赞失败喵: " + ex.Message;
        }
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("在群聊中戳一戳指定成员，返回戳一戳结果")]
    public async Task<string> PokeGroupMember(long groupId, long userId)
    {
        try
        {
            await CallActionAsync<JsonElement>("group_poke", new { group_id = groupId, user_id = userId });
            _interactor.Poke($"成功戳了戳 {userId} 喵！");
            return $"成功戳了戳 {userId} 喵！";
        }
        catch (Exception ex)
        {
            _interactor.Poke("戳一戳失败喵: " + ex.Message);
            return "戳一戳失败喵: " + ex.Message;
        }
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("获取最近一次自己发送的消息ID，用于测试撤回功能")]
    public async Task<string> GetLastMessageId()
    {
        string result = _lastSentMessageId == 0 ? "还没有记录到消息ID喵" : $"最近消息ID: {_lastSentMessageId}";
        _interactor.Poke(result);
        return result;
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("获取群最近消息及其ID，格式：群号:最近N条，返回消息内容与ID列表")]
    public async Task<string> GetGroupMessageHistory(long groupId, int count = 3)
    {
        try
        {
            var response = await CallActionAsync<JsonElement>("get_group_msg_history", new
            {
                group_id = groupId,
                count = Math.Min(count, 10)
            });
            if (!response.TryGetProperty("messages", out var messagesElement))
            {
                throw new Exception("返回中没有messages字段");
            }

            var lines = new List<string>();
            int index = 0;
            foreach (JsonElement item in messagesElement.EnumerateArray())
            {
                index++;
                long msgId = item.TryGetProperty("message_id", out var msgIdElement) ? msgIdElement.GetInt64() : 0;
                long userId = item.TryGetProperty("user_id", out var userIdElement) ? userIdElement.GetInt64() : 0;
                string rawMsg = item.TryGetProperty("raw_message", out var rawElement) ? rawElement.GetString() ?? "" : "";
                lines.Add($"{index}. id={msgId} user={userId} content={rawMsg}");
            }

            string result = "群消息历史:\n" + string.Join("\n", lines);
            _interactor.Poke(result);
            return result;
        }
        catch (Exception ex)
        {
            _interactor.Poke("获取群消息失败喵: " + ex.Message);
            return "获取群消息失败喵: " + ex.Message;
        }
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("撤回一条消息（需提供消息ID），返回撤回结果")]
    public async Task<string> DeleteMessage(int messageId)
    {
        try
        {
            await CallActionAsync<JsonElement>("delete_msg", new { message_id = messageId });
            _interactor.Poke("消息撤回成功喵！");
            return "消息撤回成功喵！";
        }
        catch (Exception ex)
        {
            _interactor.Poke("撤回失败喵: " + ex.Message);
            return "撤回失败喵: " + ex.Message;
        }
    }

    #endregion

    #region ===== QQ空间 =====

    private async Task<string> GetCookiesAsync(bool forceRefresh = false)
    {
        await _cookieLock.WaitAsync();
        try
        {
            if (!forceRefresh && _cachedCookies != null &&
                (DateTime.Now - _cookieFetchTime).TotalMinutes < Configuration.CookieRefreshMinutes)
            {
                return _cachedCookies;
            }

            var response = await CallActionAsync<JsonElement>("get_cookies", new { domain = "user.qzone.qq.com" });
            string cookies = response.ValueKind == JsonValueKind.Object && response.TryGetProperty("cookies", out var cookieElement)
                ? cookieElement.GetString() ?? ""
                : response.ValueKind == JsonValueKind.String
                    ? response.GetString() ?? ""
                    : "";
            if (string.IsNullOrEmpty(cookies))
            {
                throw new Exception("获取QQ空间Cookie失败：返回为空");
            }

            _cachedCookies = cookies;
            _cookieFetchTime = DateTime.Now;
            _logger.LogInformation("已刷新QQ空间Cookie");
            return cookies;
        }
        finally
        {
            _cookieLock.Release();
        }
    }

    private static string CalcGtk(string key)
    {
        long hash = 5381L;
        foreach (char c in key)
        {
            hash += (hash << 5) + c;
        }
        return ((int)(hash & 0x7FFFFFFF)).ToString();
    }

    private async Task<(string cookies, string gtk, long uin)> GetContextAsync()
    {
        string cookies = await GetCookiesAsync();
        var dict = ParseCookies(cookies);
        long uin = long.TryParse(dict.GetValueOrDefault("uin", "").TrimStart('o'), out var parsedUin) ? parsedUin : 0;
        string skey = dict.GetValueOrDefault("skey", "");
        string pSkey = dict.GetValueOrDefault("p_skey", "");
        string gtk = CalcGtk(!string.IsNullOrEmpty(pSkey) ? pSkey : skey);
        return (cookies, gtk, uin);
    }

    private static Dictionary<string, string> ParseCookies(string cookieStr)
    {
        var dict = new Dictionary<string, string>();
        foreach (string part in cookieStr.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int eqIndex = part.IndexOf('=');
            if (eqIndex > 0)
            {
                string key = part[..eqIndex].Trim();
                string value = part[(eqIndex + 1)..].Trim();
                dict[key] = value;
            }
        }
        return dict;
    }

    private HttpRequestMessage BuildRequest(string url, Dictionary<string, string> formData, string cookies, string gtk, string referer)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, url + "?g_tk=" + gtk);
        request.Headers.TryAddWithoutValidation("Cookie", cookies);
        request.Headers.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36");
        request.Headers.TryAddWithoutValidation("Referer", referer);
        request.Headers.TryAddWithoutValidation("Origin", "https://user.qzone.qq.com");
        request.Headers.TryAddWithoutValidation("Accept", "application/json, text/javascript, */*; q=0.01");
        request.Headers.TryAddWithoutValidation("X-Requested-With", "XMLHttpRequest");
        request.Content = new FormUrlEncodedContent(formData);
        return request;
    }

    private async Task<string> GetQzoneTokenAsync(string cookies, long uin)
    {
        try
        {
            var request = new HttpRequestMessage(HttpMethod.Get, $"https://user.qzone.qq.com/{uin}");
            request.Headers.TryAddWithoutValidation("Cookie", cookies);
            request.Headers.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36");
            request.Headers.TryAddWithoutValidation("Referer", $"https://user.qzone.qq.com/{uin}");

            using var response = await Http.SendAsync(request);
            string html = await response.Content.ReadAsStringAsync();

            string token = "";
            int index = html.IndexOf("\"qzonetoken\"", StringComparison.Ordinal);
            if (index >= 0)
            {
                int colonIndex = html.IndexOf(':', index) + 1;
                while (colonIndex < html.Length && (html[colonIndex] == ' ' || html[colonIndex] == '\t')) colonIndex++;
                if (colonIndex < html.Length && html[colonIndex] == '"')
                {
                    int start = colonIndex + 1;
                    int end = html.IndexOf('"', start);
                    if (end > start)
                    {
                        token = html[start..end];
                    }
                }
            }

            if (string.IsNullOrEmpty(token))
            {
                int eqIndex = html.IndexOf("qzonetoken=", StringComparison.Ordinal);
                if (eqIndex >= 0)
                {
                    int start = eqIndex + "qzonetoken=".Length;
                    int end = start;
                    while (end < html.Length && html[end] != '"' && html[end] != '\'' && html[end] != '&' && html[end] != '<') end++;
                    if (end > start)
                    {
                        token = html[start..end];
                    }
                }
            }

            _logger.LogInformation("QZone qzonetoken: {Status}", string.IsNullOrEmpty(token) ? "(空)" : "获取成功");
            return token;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "获取QZone qzonetoken失败");
            return "";
        }
    }

    private static async Task<string> SendAndCheckAsync(HttpRequestMessage request)
    {
        using var response = await Http.SendAsync(request);
        string body = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
        {
            throw new Exception($"HTTP {(int)response.StatusCode}: {body}");
        }

        using var doc = JsonDocument.Parse(body);
        int code = doc.RootElement.TryGetProperty("code", out var codeElement) ? codeElement.GetInt32() : -1;
        if (code != 0)
        {
            string msg = doc.RootElement.TryGetProperty("message", out var msgElement) ? msgElement.GetString() ?? "" : "";
            throw new Exception($"QZone接口错误: code={code} msg={msg}");
        }
        return body;
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("发表QQ空间说说（纯文字），返回发布结果")]
    public async Task<string> PublishMood(string content)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(content))
            {
                _interactor.Poke("内容不能为空喵！");
                return "内容不能为空喵！";
            }

            var (cookies, gtk, uin) = await GetContextAsync();
            string url = "https://user.qzone.qq.com/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_publish_v6";
            var form = new Dictionary<string, string>
            {
                ["syn_tweet_verson"] = "1",
                ["paramstr"] = "1",
                ["who"] = "1",
                ["con"] = content,
                ["feedversion"] = "1",
                ["ver"] = "1",
                ["ugc_right"] = "1",
                ["to_sign"] = "0",
                ["hostuin"] = uin.ToString(),
                ["code_version"] = "1",
                ["format"] = "json",
                ["qzreferrer"] = $"https://user.qzone.qq.com/{uin}"
            };
            await SendAndCheckAsync(BuildRequest(url, form, cookies, gtk, $"https://user.qzone.qq.com/{uin}"));
            _logger.LogInformation("QZone发说说成功: {Content}", content);
            _interactor.Poke("说说发布成功喵！✨");
            return "说说发布成功喵！✨";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "QZone发说说失败");
            _interactor.Poke("发说说失败喵: " + ex.Message);
            return "发说说失败喵: " + ex.Message;
        }
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("点赞QQ空间说说（需提供作者QQ号和说说tid），返回点赞结果")]
    public async Task<string> LikeMood(long authorUin, string tid)
    {
        try
        {
            var (cookies, gtk, uin) = await GetContextAsync();
            string unikey = $"http://user.qzone.qq.com/{authorUin}/mood/{tid}.1";
            string url = "https://user.qzone.qq.com/proxy/domain/w.qzone.qq.com/cgi-bin/likes/internal_dolike_app";
            var form = new Dictionary<string, string>
            {
                ["qzreferrer"] = $"https://user.qzone.qq.com/{authorUin}",
                ["opuin"] = authorUin.ToString(),
                ["unikey"] = unikey,
                ["curkey"] = unikey,
                ["from"] = "-100",
                ["fupdate"] = "1",
                ["face"] = "0",
                ["format"] = "json"
            };
            await SendAndCheckAsync(BuildRequest(url, form, cookies, gtk, $"https://user.qzone.qq.com/{authorUin}"));
            _logger.LogInformation("QZone点赞成功: tid={Tid}", tid);
            _interactor.Poke("点赞成功喵！❤️");
            return "点赞成功喵！❤️";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "QZone点赞失败: tid={Tid}", tid);
            _interactor.Poke("点赞失败喵: " + ex.Message);
            return "点赞失败喵: " + ex.Message;
        }
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("取消点赞QQ空间说说（需提供作者QQ号和说说tid），返回取消结果")]
    public async Task<string> UnlikeMood(long authorUin, string tid)
    {
        try
        {
            var (cookies, gtk, uin) = await GetContextAsync();
            string unikey = $"http://user.qzone.qq.com/{authorUin}/mood/{tid}.1";
            string url = "https://user.qzone.qq.com/proxy/domain/w.qzone.qq.com/cgi-bin/likes/internal_unlike_app";
            var form = new Dictionary<string, string>
            {
                ["qzreferrer"] = $"https://user.qzone.qq.com/{authorUin}",
                ["opuin"] = authorUin.ToString(),
                ["unikey"] = unikey,
                ["curkey"] = unikey,
                ["from"] = "-100",
                ["fupdate"] = "1",
                ["face"] = "0",
                ["format"] = "json"
            };
            await SendAndCheckAsync(BuildRequest(url, form, cookies, gtk, $"https://user.qzone.qq.com/{authorUin}"));
            _logger.LogInformation("QZone取消点赞成功: tid={Tid}", tid);
            _interactor.Poke("取消点赞成功喵");
            return "取消点赞成功喵";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "QZone取消点赞失败: tid={Tid}", tid);
            _interactor.Poke("取消点赞失败喵: " + ex.Message);
            return "取消点赞失败喵: " + ex.Message;
        }
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("获取QQ空间最新说说列表（默认自己的，可指定他人QQ号），返回最近几条说说的内容")]
    public async Task<string> GetMoodList(long uin = 0L)
    {
        try
        {
            var (cookies, gtk, myUin) = await GetContextAsync();
            long targetUin = uin > 0 ? uin : myUin;
            string qzonetoken = await GetQzoneTokenAsync(cookies, targetUin);

            string url = "https://h5.qzone.qq.com/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6";
            var queryParams = new Dictionary<string, string>
            {
                ["sort"] = "0",
                ["start"] = "0",
                ["num"] = "10",
                ["cgi_host"] = "http://taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6",
                ["replynum"] = "100",
                ["callback"] = "_preloadCallback",
                ["code_version"] = "1",
                ["inCharset"] = "utf-8",
                ["outCharset"] = "utf-8",
                ["notice"] = "0",
                ["format"] = "jsonp",
                ["need_private_comment"] = "1",
                ["g_tk"] = gtk,
                ["qzonetoken"] = qzonetoken,
                ["uin"] = targetUin.ToString(),
                ["pos"] = "0"
            };
            var queryString = new StringBuilder();
            foreach (var (key, value) in queryParams)
            {
                if (queryString.Length > 0) queryString.Append('&');
                queryString.Append(key).Append('=').Append(Uri.EscapeDataString(value));
            }

            var request = new HttpRequestMessage(HttpMethod.Get, url + "?" + queryString);
            request.Headers.TryAddWithoutValidation("Cookie", cookies);
            request.Headers.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36");
            request.Headers.TryAddWithoutValidation("Referer", $"https://user.qzone.qq.com/{targetUin}");
            request.Headers.TryAddWithoutValidation("Accept", "application/json, text/javascript, */*; q=0.01");
            request.Headers.TryAddWithoutValidation("X-Requested-With", "XMLHttpRequest");
            request.Headers.TryAddWithoutValidation("Origin", "https://user.qzone.qq.com");

            using var response = await Http.SendAsync(request);
            string body = await response.Content.ReadAsStringAsync();

            // 解析JSONP
            string json = body;
            if (body.Contains("_preloadCallback("))
            {
                int start = body.IndexOf('(') + 1;
                int end = body.LastIndexOf(')');
                if (start > 0 && end > start)
                {
                    json = body[start..end];
                }
            }

            using var doc = JsonDocument.Parse(json);
            int code = doc.RootElement.TryGetProperty("code", out var codeElement) ? codeElement.GetInt32() : -1;
            if (code != 0)
            {
                string msg = doc.RootElement.TryGetProperty("message", out var msgElement) ? msgElement.GetString() ?? "" : "";
                _interactor.Poke($"获取说说列表失败喵: code={code} msg={msg}");
                return $"获取说说列表失败喵: code={code} msg={msg}";
            }
            if (!doc.RootElement.TryGetProperty("msglist", out var msgList))
            {
                _interactor.Poke("没有获取到说说喵");
                return "没有获取到说说喵";
            }

            var result = new StringBuilder();
            int count = 0;
            foreach (JsonElement item in msgList.EnumerateArray())
            {
                if (count >= 10) break;
                string tid = item.TryGetProperty("tid", out var tidElement) ? tidElement.GetString() ?? "" : "";
                string content = item.TryGetProperty("content", out var contentElement) ? contentElement.GetString() ?? "" : "";
                string createTime = item.TryGetProperty("createTime", out var timeElement) ? timeElement.GetString() ?? "" : "";
                string name = item.TryGetProperty("name", out var nameElement) ? nameElement.GetString() ?? "" : "";
                if (content.Length > 80)
                {
                    content = content[..80] + "...";
                }
                if (count > 0) result.AppendLine();
                result.Append($"{count + 1}. [{name}] {content} ({createTime}) tid={tid}");
                count++;
            }

            _logger.LogInformation("QZone获取说说列表成功: {Count}条", count);
            string output = $"获取到{count}条说说喵：\n{result}";
            _interactor.Poke(output);
            return output;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "QZone获取说说列表失败");
            _interactor.Poke("获取说说列表失败喵: " + ex.Message);
            return "获取说说列表失败喵: " + ex.Message;
        }
    }

    [XmlFunction(FunctionMode.OneShot, null, 0)]
    [Description("删除QQ空间说说（需提供tid）")]
    public async Task<string> DeleteMood(string tid)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(tid))
            {
                _interactor.Poke("tid不能为空喵！");
                return "tid不能为空喵！";
            }

            var (cookies, gtk, uin) = await GetContextAsync();
            string url = "https://user.qzone.qq.com/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_delete_v6";
            var form = new Dictionary<string, string>
            {
                ["hostuin"] = uin.ToString(),
                ["tid"] = tid,
                ["code_version"] = "1",
                ["format"] = "json",
                ["qzreferrer"] = $"https://user.qzone.qq.com/{uin}"
            };
            await SendAndCheckAsync(BuildRequest(url, form, cookies, gtk, $"https://user.qzone.qq.com/{uin}"));
            _logger.LogInformation("QZone删除说说成功: tid={Tid}", tid);
            _interactor.Poke("删除说说成功喵！tid=" + tid);
            return "删除说说成功喵！tid=" + tid;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "QZone删除说说失败: tid={Tid}", tid);
            _interactor.Poke("删除说说失败喵: " + ex.Message);
            return "删除说说失败喵: " + ex.Message;
        }
    }

    #endregion
}
