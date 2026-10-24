下面是一道接近真实互联网公司 **C++ 后端 / 平台工程师 AI Coding 面试题**。题目以 API Gateway 限流为背景，要求候选人可以使用 AI 辅助，但必须说明设计、审查 AI 生成内容、处理并发和故障场景，并写出可测试实现。

---

# 面试题：实现 C++ 分布式 Token Bucket Rate Limiter

## 业务背景

公司有一个 C++ API Gateway，运行多个实例，所有请求会被负载均衡到不同机器。

目前接口缺少统一限流，存在：

- 用户或爬虫高频调用，压垮下游；
- 登录接口容易遭受密码爆破；
- 单机内存限流在多实例环境中失效；
- Redis 异常时，不同接口需要不同降级策略。

请为网关实现一个可复用的**分布式限流器**。

---

## 功能要求

1. 使用 **Redis** 存储共享限流状态；
2. 使用 **Token Bucket（令牌桶）** 算法；
3. 限流状态必须支持多 Gateway 实例共享；
4. 整个“补充令牌 → 判断 → 扣减 → 更新状态”的过程必须原子；
5. 登录用户按 `userId` 限流；匿名用户按真实客户端 IP 限流；
6. 支持路由级策略：
   - `/api/orders`：平均 `60 req/min`，最大突发 `20`；
   - `/api/login`：平均 `5 req/min`，最大突发 `5`；
7. 被限流时返回：
   - HTTP `429 Too Many Requests`
   - `RateLimit-Limit`
   - `RateLimit-Remaining`
   - `RateLimit-Reset`
   - `Retry-After`
8. Redis 不可用时：
   - 登录接口：`fail-closed`，返回 `503`；
   - 普通订单查询接口：`fail-open`，放行但记录监控；
9. 需要提供并发测试；
10. C++17 或更新标准均可。

---

# 一、面试开始时应先澄清的问题

成熟的候选人不要马上让 AI “写一个 Redis 限流器”，而应先确认约束。

例如：

> 1. 网关是否部署在可信反向代理之后？  
> 2. 是否已经正确配置了真实客户端 IP 的提取方式？  
> 3. Redis 是单节点、Sentinel 还是 Cluster？  
> 4. 是否允许同一个用户在不同 API 上共享额度？  
> 5. 规则是否需要动态配置，例如从配置中心热更新？  
> 6. “60 req/min，突发 20”是指平均速率 60/min、桶容量 20 吗？  
> 7. Redis 故障时，是否允许本地内存限流作为兜底？  
> 8. 是否要求严格保证“任意 60 秒不超过 60 次”？  

最后一个问题很关键：

- 如果要求**平均速率 + 短时突发**：Token Bucket 合适；
- 如果要求**任意滑动窗口内严格不超过 N 次**：更适合 Sliding Window；
- 如果要限制**并发处理数**：应使用 Semaphore，而不是 Token Bucket。

这里需求明确采用 Token Bucket。

---

# 二、设计思路

## 1. 为什么不能用进程内存？

如果有三个 Gateway 实例：

```text
Gateway A：允许 user-123 再请求 20 次
Gateway B：允许 user-123 再请求 20 次
Gateway C：允许 user-123 再请求 20 次
```

用户实际上可能在瞬间通过 60 次请求。

因此状态必须放在共享存储中，例如 Redis。

---

## 2. 为什么不能用 `GET` + C++ 计算 + `SET`？

假设桶里只剩一个令牌：

```text
请求 A：GET，读到 1 个令牌
请求 B：GET，读到 1 个令牌
请求 A：判断通过，写回 0
请求 B：判断通过，写回 0
```

结果：一个令牌放行了两个请求。

因此必须把以下操作放到一个原子单元中：

```text
读取桶状态
→ 计算经过时间并补充令牌
→ 判断令牌是否足够
→ 扣减令牌
→ 写回新状态和 TTL
```

Redis Lua Script 在 Redis 中原子执行，适合这个需求。

---

## 3. 为什么使用 Redis `TIME`？

不能使用每个 C++ 实例自己的 `std::chrono::system_clock` 作为补充令牌的时间依据。

因为不同机器可能有时钟漂移：

```text
Gateway A：系统时间快 2 秒
Gateway B：系统时间慢 1 秒
```

如果各实例自己计算令牌，就可能过度补充或少补充。

Redis `TIME` 让同一 Redis 实例上的限流桶统一基于 Redis 服务端时间计算。

---

# 三、合理使用 AI 的 Prompt

候选人可以向 AI 提出类似请求：

```text name=ai-prompts/token_bucket_design.txt
我正在为 C++17 API Gateway 实现 Redis 分布式 Token Bucket 限流器。

要求：
1. Redis Lua Script 必须原子完成令牌补充、判断、扣减、写回和设置 TTL。
2. 使用 Redis TIME，不能使用应用服务器时间。
3. 限流规则支持 capacity、每个周期补充的令牌数、周期长度和单次 cost。
4. 支持 60 req/min、突发 20，以及 5 req/min、突发 5。
5. 返回 allowed、remaining、retryAfterMs。
6. 请避免 Lua 浮点精度和 Redis Lua 返回浮点数被截断的问题。
7. 先解释状态设计和边界条件，再给 Lua 脚本与 C++ 调用接口。
```

这个 Prompt 比“帮我写限流器”更好，因为它明确要求 AI 处理：

- 分布式；
- 原子性；
- Redis 时间；
- 非整除速率；
- Lua 数值精度；
- 返回值语义；
- C++ 集成边界。

---

# 四、审查 AI 常见错误答案

AI 很可能生成这种代码：

```cpp name=bad_rate_limiter.cpp
bool AllowRequest(redisContext* context, const std::string& key, int limit) {
    auto* reply = static_cast<redisReply*>(
        redisCommand(context, "INCR %s", key.c_str())
    );

    const bool allowed = reply->integer <= limit;
    freeReplyObject(reply);

    return allowed;
}
```

这段代码不能用于本题，原因包括：

1. **这只是计数器，不是 Token Bucket。**
2. **没有过期时间。** Redis key 会无限积累。
3. **没有突发容量和补充速率。**
4. **无法计算 `Retry-After`。**
5. **没有区分用户、IP、路由。**
6. **没有 Redis 故障策略。**
7. 如果改成 `INCR` 后再 `EXPIRE`，两个命令之间仍可能出错。
8. 无法满足“多请求成本”或“不同接口规则”。
9. 固定窗口算法还会有窗口边界突发问题。

好的候选人应该明确说：

> 这段代码可以作为非常粗粒度的固定窗口限流原型，但不满足题目要求。我会使用 Redis Lua Script 实现原子 Token Bucket。

---

# 五、核心设计：避免 Lua 浮点问题

Lua 和 Redis Lua Script 在处理浮点数时容易产生隐患：

- 浮点数存在精度误差；
- Redis Lua 返回数值时可能产生截断或非预期结果；
- `5 req/min` 等速率无法简单转换为“每毫秒整数令牌”。

因此这里使用**整数单位**。

## 状态定义

每个桶在 Redis Hash 中保存：

| 字段 | 含义 |
|---|---|
| `tokens` | 当前令牌数，单位为 milli-token，即 1 个令牌 = 1000 |
| `lastRefillMs` | 上次补充令牌时的 Redis 时间戳，毫秒 |
| `remainder` | 上次补充计算留下的余数，避免低速率令牌被截断丢失 |

例如：

```text
capacity = 20 tokens
capacityUnits = 20 * 1000 = 20000

60 req/min:
refillUnits = 60 * 1000 = 60000
periodMs = 60000

5 req/min:
refillUnits = 5 * 1000 = 5000
periodMs = 60000
```

补充公式：

```text
accumulated = elapsedMs * refillUnits + remainder
addedTokens = accumulated / periodMs
newRemainder = accumulated % periodMs
```

这样即使 `5 req/min` 不能整除到每毫秒，也不会在每次请求时丢失小数部分。

---

# 六、Lua Script：原子 Token Bucket

```lua name=src/rate_limit/token_bucket.lua
-- KEYS[1] = bucket key
--
-- ARGV[1] = capacityUnits, e.g. 20000 for 20 tokens
-- ARGV[2] = refillUnits, e.g. 60000 for 60 tokens
-- ARGV[3] = periodMs, e.g. 60000
-- ARGV[4] = costUnits, e.g. 1000 for one request
-- ARGV[5] = ttlMs

local key = KEYS[1]

local capacityUnits = tonumber(ARGV[1])
local refillUnits = tonumber(ARGV[2])
local periodMs = tonumber(ARGV[3])
local costUnits = tonumber(ARGV[4])
local ttlMs = tonumber(ARGV[5])

if capacityUnits <= 0 or periodMs <= 0 or costUnits <= 0 then
    return {0, 0, -1}
end

if costUnits > capacityUnits then
    -- 单次请求成本超过桶容量，永远不可能成功。
    return {0, 0, -1}
end

local now = redis.call("TIME")
local nowMs = tonumber(now[1]) * 1000
    + math.floor(tonumber(now[2]) / 1000)

local state = redis.call(
    "HMGET",
    key,
    "tokens",
    "lastRefillMs",
    "remainder"
)

local tokens = tonumber(state[1])
local lastRefillMs = tonumber(state[2])
local remainder = tonumber(state[3])

-- 新桶初始为满桶。
if tokens == nil or lastRefillMs == nil or remainder == nil then
    tokens = capacityUnits
    lastRefillMs = nowMs
    remainder = 0
end

local elapsedMs = math.max(0, nowMs - lastRefillMs)

if refillUnits > 0 and elapsedMs > 0 then
    local accumulated = elapsedMs * refillUnits + remainder
    local addedUnits = math.floor(accumulated / periodMs)

    remainder = accumulated % periodMs
    tokens = math.min(capacityUnits, tokens + addedUnits)
    lastRefillMs = nowMs

    -- 如果已满桶，之前的补充余数没有意义。
    if tokens == capacityUnits then
        remainder = 0
    end
end

local allowed = 0
local retryAfterMs = 0

if tokens >= costUnits then
    tokens = tokens - costUnits
    allowed = 1
elseif refillUnits <= 0 then
    -- 永不补充令牌，且当前令牌不足。
    retryAfterMs = -1
else
    local missingUnits = costUnits - tokens
    local numerator = missingUnits * periodMs - remainder

    -- 取向上整，得到最少等待毫秒数。
    retryAfterMs = math.max(
        1,
        math.ceil(numerator / refillUnits)
    )
end

redis.call(
    "HSET",
    key,
    "tokens",
    tokens,
    "lastRefillMs",
    lastRefillMs,
    "remainder",
    remainder
)

redis.call("PEXPIRE", key, ttlMs)

return {allowed, tokens, retryAfterMs}
```

---

# 七、C++17 数据模型

以下示例使用一个抽象 Redis 接口，以便测试时替换为 fake Redis；生产中可以用 `redis-plus-plus`、`hiredis` 或公司内部 Redis SDK 实现。

```cpp name=src/rate_limit/redis_client.h
#pragma once

#include <string>
#include <vector>

namespace gateway {

class RedisClient {
public:
    virtual ~RedisClient() = default;

    // 返回 Lua 脚本 SHA。
    virtual std::string ScriptLoad(const std::string& script) = 0;

    // 返回 Redis Lua 的整数数组结果。
    virtual std::vector<long long> EvalSha(
        const std::string& sha,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& args
    ) = 0;
};

}  // namespace gateway
```

限流规则和结果：

```cpp name=src/rate_limit/token_bucket_rate_limiter.h
#pragma once

#include "redis_client.h"

#include <cstdint>
#include <optional>
#include <string>

namespace gateway {

struct RateLimitPolicy {
    std::string name;

    // 桶容量，例如 20。
    std::int64_t capacityTokens;

    // 每 periodMs 补充 refillTokens 个令牌。
    // 例如 60 req/min: refillTokens = 60, periodMs = 60000。
    std::int64_t refillTokens;
    std::int64_t periodMs;

    // 单次请求消耗令牌数，通常为 1。
    std::int64_t costTokens{1};

    // Redis 异常时的降级行为。
    enum class FailureMode {
        kFailOpen,
        kFailClosed,
    };

    FailureMode failureMode{FailureMode::kFailClosed};
};

struct RateLimitResult {
    bool allowed{false};
    std::int64_t remainingTokens{0};

    // nullopt 表示不会自动恢复，例如 refillTokens == 0。
    std::optional<std::int64_t> retryAfterMs;

    // 到桶完全恢复所需秒数。
    std::optional<std::int64_t> resetAfterSeconds;
};

class TokenBucketRateLimiter {
public:
    TokenBucketRateLimiter(
        RedisClient& redis,
        std::string luaScript
    );

    RateLimitResult Consume(
        const std::string& key,
        const RateLimitPolicy& policy
    );

private:
    static constexpr std::int64_t kTokenScale = 1000;

    std::int64_t CalculateTtlMs(const RateLimitPolicy& policy) const;
    void ValidatePolicy(const RateLimitPolicy& policy) const;

    RedisClient& redis_;
    std::string script_;
    std::string scriptSha_;
};

}  // namespace gateway
```

---

# 八、C++17 实现

```cpp name=src/rate_limit/token_bucket_rate_limiter.cpp
#include "token_bucket_rate_limiter.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>
#include <vector>

namespace gateway {
namespace {

std::string ToString(std::int64_t value) {
    return std::to_string(value);
}

}  // namespace

TokenBucketRateLimiter::TokenBucketRateLimiter(
    RedisClient& redis,
    std::string luaScript
)
    : redis_(redis),
      script_(std::move(luaScript)),
      scriptSha_(redis_.ScriptLoad(script_)) {}

void TokenBucketRateLimiter::ValidatePolicy(
    const RateLimitPolicy& policy
) const {
    if (policy.name.empty()) {
        throw std::invalid_argument("Rate-limit policy name must not be empty.");
    }

    if (policy.capacityTokens <= 0) {
        throw std::invalid_argument("capacityTokens must be positive.");
    }

    if (policy.refillTokens < 0) {
        throw std::invalid_argument("refillTokens must not be negative.");
    }

    if (policy.periodMs <= 0) {
        throw std::invalid_argument("periodMs must be positive.");
    }

    if (policy.costTokens <= 0) {
        throw std::invalid_argument("costTokens must be positive.");
    }

    if (policy.costTokens > policy.capacityTokens) {
        throw std::invalid_argument(
            "costTokens cannot exceed capacityTokens."
        );
    }
}

std::int64_t TokenBucketRateLimiter::CalculateTtlMs(
    const RateLimitPolicy& policy
) const {
    constexpr std::int64_t kMinimumTtlMs = 60'000;
    constexpr std::int64_t kZeroRefillTtlMs = 60 * 60 * 1000;

    if (policy.refillTokens == 0) {
        return kZeroRefillTtlMs;
    }

    // 填满桶所需时间：
    // capacity / refillTokens * periodMs。
    const auto fullRefillMs = static_cast<std::int64_t>(
        std::ceil(
            static_cast<double>(policy.capacityTokens) *
            static_cast<double>(policy.periodMs) /
            static_cast<double>(policy.refillTokens)
        )
    );

    // 保留两倍时间，避免活跃桶过早过期。
    return std::max(kMinimumTtlMs, fullRefillMs * 2);
}

RateLimitResult TokenBucketRateLimiter::Consume(
    const std::string& key,
    const RateLimitPolicy& policy
) {
    ValidatePolicy(policy);

    const auto capacityUnits = policy.capacityTokens * kTokenScale;
    const auto refillUnits = policy.refillTokens * kTokenScale;
    const auto costUnits = policy.costTokens * kTokenScale;
    const auto ttlMs = CalculateTtlMs(policy);

    const std::vector<std::string> keys{key};
    const std::vector<std::string> args{
        ToString(capacityUnits),
        ToString(refillUnits),
        ToString(policy.periodMs),
        ToString(costUnits),
        ToString(ttlMs),
    };

    std::vector<long long> response;

    try {
        response = redis_.EvalSha(scriptSha_, keys, args);
    } catch (const std::exception& error) {
        // 生产实现中，应仅对 Redis NOSCRIPT 错误执行重载。
        // 此处简化为重新加载后重试一次。
        scriptSha_ = redis_.ScriptLoad(script_);
        response = redis_.EvalSha(scriptSha_, keys, args);
    }

    if (response.size() != 3) {
        throw std::runtime_error("Unexpected Token Bucket Lua response.");
    }

    const bool allowed = response[0] == 1;
    const auto remainingUnits = std::max(0LL, response[1]);
    const auto retryAfterMsValue = response[2];

    RateLimitResult result;
    result.allowed = allowed;
    result.remainingTokens = remainingUnits / kTokenScale;

    if (retryAfterMsValue >= 0) {
        result.retryAfterMs = retryAfterMsValue;
    }

    if (policy.refillTokens > 0) {
        const auto missingUnits = std::max(
            0LL,
            capacityUnits - remainingUnits
        );

        const auto resetAfterMs = static_cast<std::int64_t>(
            std::ceil(
                static_cast<double>(missingUnits) *
                static_cast<double>(policy.periodMs) /
                static_cast<double>(refillUnits)
            )
        );

        result.resetAfterSeconds =
            static_cast<std::int64_t>(
                std::ceil(resetAfterMs / 1000.0)
            );
    }

    return result;
}

}  // namespace gateway
```

---

# 九、构造限流 Key

Key 应包含：

1. 限流规则名；
2. 调用方身份；
3. 必要时包含租户、API Key、路由或 HTTP 方法。

```cpp name=src/rate_limit/rate_limit_key.h
#pragma once

#include <string>

namespace gateway {

struct RequestIdentity {
    std::string userId;
    std::string clientIp;
};

inline std::string BuildRateLimitKey(
    const std::string& policyName,
    const RequestIdentity& identity
) {
    const std::string subject = !identity.userId.empty()
        ? "user:" + identity.userId
        : "ip:" + identity.clientIp;

    // 花括号是 Redis Cluster hash tag。
    // 同一主体相关 key 可被路由到同一 hash slot。
    return "rate-limit:{" + policyName + ":" + subject + "}";
}

}  // namespace gateway
```

这里需要强调：

> 不能直接相信客户端传来的 `X-Forwarded-For`。只有网关在可信 Nginx、Ingress 或 Load Balancer 后，并且网络路径受控时，才应从代理头中提取真实 IP。否则攻击者可持续伪造 IP 绕过匿名限流。

---

# 十、接入 Web 框架的伪代码

不同公司可能使用 Drogon、oat++、Crow、Pistache、brpc、内部 HTTP 框架或 Envoy Filter。下面展示框架无关的中间件逻辑。

```cpp name=src/middlewares/rate_limit_middleware.cpp
#include "rate_limit_key.h"
#include "token_bucket_rate_limiter.h"

#include <chrono>
#include <cmath>
#include <string>

namespace gateway {

struct HttpRequest {
    std::string route;
    std::string userId;
    std::string clientIp;
};

struct HttpResponse {
    int statusCode{200};

    void SetHeader(const std::string& name, const std::string& value);
    void SetJsonBody(const std::string& body);
};

void ApplyRateLimitHeaders(
    HttpResponse& response,
    const RateLimitPolicy& policy,
    const RateLimitResult& result
) {
    response.SetHeader(
        "RateLimit-Limit",
        std::to_string(policy.capacityTokens)
    );

    response.SetHeader(
        "RateLimit-Remaining",
        std::to_string(result.remainingTokens)
    );

    if (result.resetAfterSeconds.has_value()) {
        const auto nowEpochSeconds =
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();

        response.SetHeader(
            "RateLimit-Reset",
            std::to_string(nowEpochSeconds + *result.resetAfterSeconds)
        );
    }

    if (result.retryAfterMs.has_value()) {
        const auto retryAfterSeconds = std::max(
            1LL,
            static_cast<long long>(
                std::ceil(*result.retryAfterMs / 1000.0)
            )
        );

        response.SetHeader(
            "Retry-After",
            std::to_string(retryAfterSeconds)
        );
    }
}

bool CheckRateLimit(
    const HttpRequest& request,
    HttpResponse& response,
    TokenBucketRateLimiter& limiter,
    const RateLimitPolicy& policy
) {
    const RequestIdentity identity{
        .userId = request.userId,
        .clientIp = request.clientIp,
    };

    try {
        const auto key = BuildRateLimitKey(policy.name, identity);
        const auto result = limiter.Consume(key, policy);

        ApplyRateLimitHeaders(response, policy, result);

        if (result.allowed) {
            return true;
        }

        response.statusCode = 429;
        response.SetJsonBody(
            R"({"code":"RATE_LIMITED","message":"Too many requests."})"
        );
        return false;
    } catch (const std::exception&) {
        if (policy.failureMode == RateLimitPolicy::FailureMode::kFailOpen) {
            // 真实环境还需打日志、监控和告警。
            return true;
        }

        response.statusCode = 503;
        response.SetJsonBody(
            R"({"code":"RATE_LIMIT_UNAVAILABLE",)"
            R"("message":"Request protection is temporarily unavailable."})"
        );
        return false;
    }
}

}  // namespace gateway
```

---

# 十一、路由策略配置

```cpp name=src/config/rate_limit_policies.cpp
#include "token_bucket_rate_limiter.h"

namespace gateway {

const RateLimitPolicy kLoginPolicy{
    .name = "login",
    .capacityTokens = 5,
    .refillTokens = 5,
    .periodMs = 60'000,
    .costTokens = 1,
    .failureMode = RateLimitPolicy::FailureMode::kFailClosed,
};

const RateLimitPolicy kOrdersPolicy{
    .name = "orders",
    .capacityTokens = 20,
    .refillTokens = 60,
    .periodMs = 60'000,
    .costTokens = 1,
    .failureMode = RateLimitPolicy::FailureMode::kFailOpen,
};

}  // namespace gateway
```

解释：

| 接口 | 桶容量 | 补充规则 | 效果 |
|---|---:|---:|---|
| `/api/login` | 5 | 每 60 秒补充 5 个 | 最多立即请求 5 次，长期平均每分钟 5 次 |
| `/api/orders` | 20 | 每 60 秒补充 60 个 | 最多立即请求 20 次，长期平均每分钟 60 次 |

特别容易被问到的问题：

> 为什么 `/api/orders` 不是 `capacity=60`？

回答：

> 因为需求规定平均速率是 60 req/min，但最大突发是 20。Token Bucket 中，平均速率由补充速率控制，突发能力由桶容量控制。设为容量 60 会允许用户空闲一段时间后瞬发 60 次，这不符合突发上限 20 的要求。

---

# 十二、关键测试

真实项目建议使用 Testcontainers 或 Docker 启动真实 Redis 运行集成测试，因为仅 Mock Redis 无法验证 Lua 脚本原子性。

下面是 GoogleTest 风格的测试示例。

```cpp name=tests/token_bucket_rate_limiter_test.cpp
#include "rate_limit/token_bucket_rate_limiter.h"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <string>
#include <vector>

namespace gateway {
namespace {

class RealRedisClientForTest final : public RedisClient {
public:
    std::string ScriptLoad(const std::string& script) override;
    std::vector<long long> EvalSha(
        const std::string& sha,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& args
    ) override;
};

TEST(TokenBucketRateLimiterTest, AllowsRequestsUntilCapacityIsExhausted) {
    RealRedisClientForTest redis;
    TokenBucketRateLimiter limiter(redis, LoadTokenBucketLuaScriptForTest());

    const RateLimitPolicy policy{
        .name = "test-basic",
        .capacityTokens = 3,
        .refillTokens = 0,
        .periodMs = 60'000,
        .costTokens = 1,
        .failureMode = RateLimitPolicy::FailureMode::kFailClosed,
    };

    const std::string key = "test-rate-limit:{basic:user:1}";

    EXPECT_TRUE(limiter.Consume(key, policy).allowed);
    EXPECT_TRUE(limiter.Consume(key, policy).allowed);
    EXPECT_TRUE(limiter.Consume(key, policy).allowed);

    const auto denied = limiter.Consume(key, policy);

    EXPECT_FALSE(denied.allowed);
    EXPECT_EQ(denied.remainingTokens, 0);
    EXPECT_FALSE(denied.retryAfterMs.has_value());
}

TEST(TokenBucketRateLimiterTest, SupportsWeightedCosts) {
    RealRedisClientForTest redis;
    TokenBucketRateLimiter limiter(redis, LoadTokenBucketLuaScriptForTest());

    const RateLimitPolicy policy{
        .name = "test-cost",
        .capacityTokens = 10,
        .refillTokens = 0,
        .periodMs = 60'000,
        .costTokens = 7,
        .failureMode = RateLimitPolicy::FailureMode::kFailClosed,
    };

    const std::string key = "test-rate-limit:{cost:user:1}";

    const auto first = limiter.Consume(key, policy);
    EXPECT_TRUE(first.allowed);
    EXPECT_EQ(first.remainingTokens, 3);

    const auto second = limiter.Consume(key, policy);
    EXPECT_FALSE(second.allowed);
}

TEST(TokenBucketRateLimiterTest, AllowsAtMostCapacityUnderConcurrency) {
    RealRedisClientForTest redis;
    TokenBucketRateLimiter limiter(redis, LoadTokenBucketLuaScriptForTest());

    const RateLimitPolicy policy{
        .name = "test-concurrency",
        .capacityTokens = 10,
        .refillTokens = 0,
        .periodMs = 60'000,
        .costTokens = 1,
        .failureMode = RateLimitPolicy::FailureMode::kFailClosed,
    };

    const std::string key = "test-rate-limit:{concurrency:user:1}";

    std::vector<std::future<RateLimitResult>> futures;

    for (int index = 0; index < 100; ++index) {
        futures.emplace_back(
            std::async(std::launch::async, [&limiter, &key, &policy] {
                return limiter.Consume(key, policy);
            })
        );
    }

    int allowedCount = 0;

    for (auto& future : futures) {
        if (future.get().allowed) {
            ++allowedCount;
        }
    }

    EXPECT_EQ(allowedCount, 10);
}

}  // namespace
}  // namespace gateway
```

---

# 十三、面试中应主动补充的测试清单

除了代码示例，建议主动说出以下测试点：

| 测试 | 验证内容 |
|---|---|
| 新桶首次请求 | 桶是否初始为满桶 |
| 桶容量耗尽 | 是否拒绝并返回 `429` |
| 等待后再请求 | 是否按速率正确补充令牌 |
| 100 并发请求、容量 10 | 是否最多只放行 10 个 |
| `cost > capacity` | 是否明确拒绝无解请求 |
| `refillTokens = 0` | 是否正确表示永久不恢复 |
| 用户 A 与用户 B | 是否拥有独立桶 |
| 已登录用户与匿名用户 | 是否分别按用户 ID / IP 限流 |
| `/login` 与 `/orders` | 是否使用独立规则、独立 Redis key |
| Redis 失效 + fail-open | 普通接口是否继续可用且产生告警 |
| Redis 失效 + fail-closed | 登录接口是否返回 `503` |
| Lua `NOSCRIPT` | Redis 重启或脚本缓存失效后是否重载脚本 |
| TTL | 不活跃用户的 bucket 是否最终自动过期 |
| 可信代理配置 | 是否避免伪造 `X-Forwarded-For` 绕过限流 |

---

# 十四、面试官追问与参考回答

## 追问 1：为什么 Lua Script 能保证并发正确性？

Redis 会以原子方式执行单个 Lua Script。

因此不会出现：

```text
请求 A 读到剩余 1 个令牌
请求 B 也读到剩余 1 个令牌
请求 A 和 B 都成功
```

在 Lua Script 中，一个请求完整执行“读取、补充、判断、扣减、写回”后，下一个请求才能开始执行脚本。

---

## 追问 2：Redis 不是单点吗？

是的，限流依赖 Redis，因此生产方案通常需要：

- Redis Sentinel 或 Redis Cluster；
- 多可用区部署；
- 连接超时、熔断和重试策略；
- 区分 `fail-open` 与 `fail-closed`；
- 监控 Redis 延迟、错误率、CPU、连接池和��查询；
- 在 CDN、WAF 或 Envoy 层做更粗粒度的前置防护。

此外，限流通常不是唯一防线：

```text
CDN / WAF
  ↓
API Gateway 限流
  ↓
应用服务限流
  ↓
数据库连接池、队列、熔断和过载保护
```

---

## 追问 3：为什么不直接使用 Redis 的 `INCR`？

`INCR` 更适合固定窗口计数器，简单但存在窗口边界突发问题。

例如限制 `60 req/min`：

```text
12:00:59 发送 60 次
12:01:00 再发送 60 次
```

用户可能在极短时间内通过 120 次请求。

Token Bucket 的特点是：

- 允许可控突发；
- 长期平均速率稳定；
- 可以计算等待时间；
- 适合 API Gateway 流量控制。

---

## 追问 4：`RateLimit-Reset` 和 `Retry-After` 有什么区别？

- `Retry-After`：调用方最少等多久，下一次请求才可能成功；
- `RateLimit-Reset`：这里定义为桶恢复到满容量的预计 Unix 时间戳。

例如桶已经空了：

```text
Retry-After: 1
```

代表约 1 秒后可以再发一个请求。

但：

```text
RateLimit-Reset: 1760000000
```

可能表示还要更久桶才能恢复到完整容量。

---

## 追问 5：如何处理 Redis Cluster？

本题每次只操作一个 Redis key，因此 Lua 脚本本身没有多 key 跨 slot 问题。

但在 Redis Cluster 环境中，建议 key 使用 Hash Tag：

```text
rate-limit:{orders:user:123}
```

花括号中的内容用于计算 slot。若未来要在同一脚本中读取多个关联 key，应确保它们拥有相同的 hash tag，否则会遇到 `CROSSSLOT` 错误。

---

# 十五、AI Coding 面试中如何说明你的 AI 使用方式

可以在面试最后这样总结：

> 我会让 AI 帮我生成 Lua 脚本的初稿、枚举边界条件和测试场景，但不会直接接受结果。  
>  
> 我重点审查以下内容：  
> 1. 状态更新是否原子；  
> 2. 是否在多实例下共享状态；  
> 3. 是否使用统一时间源；  
> 4. Lua 是否存在浮点或整数溢出问题；  
> 5. Redis 故障时的业务降级是否合理；  
> 6. IP 身份是否能被伪造；  
> 7. 并发测试是否真的验证了“最多放行 capacity 次”。  
>  
> AI 可以缩短实现和查漏补缺的时间，但算法语义、并发正确性、安全边界和生产故障策略仍然必须由工程师负责。

---

# 这道题真正考察什么

这道题并不只是在考察“会不会写 Redis Lua”。

它实际考察：

- C++ 服务端工程能力；
- 分布式系统的一致性意识；
- 并发竞争条件分析；
- Redis 和 Lua 的正确使用；
- API 网关与安全意识；
- 降级与可用性取舍；
- 测试设计能力；
- 使用 AI 后是否仍能独立审查和解释代码。
