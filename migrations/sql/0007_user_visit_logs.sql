-- 0007_user_visit_logs.sql
-- User visit & login audit logs

CREATE TABLE IF NOT EXISTS user_visit_logs (
  id BIGSERIAL PRIMARY KEY, -- 自增主键

  user_id INT, -- 用户ID，未登录时可为空
  ip_address INET NOT NULL, -- IPv4/IPv6

  session_id VARCHAR(100), -- 会话ID，用于追踪同一批次的操作
  action_type VARCHAR(20), -- 操作类型：'login', 'visit', 'logout' 等

  -- 浏览器与设备信息
  user_agent TEXT, -- 原始用户代理字符串
  browser_family VARCHAR(50), -- 浏览器名称，如 Chrome, Firefox
  os_family VARCHAR(50), -- 操作系统，如 Windows, macOS, Android
  device_type VARCHAR(20), -- 设备类型：pc, mobile, tablet

  -- 来源与路径
  request_url TEXT NOT NULL, -- 当前请求的完整URL
  referer_url TEXT, -- 来源页面URL

  -- 扩展数据
  request_headers JSONB, -- 以JSON格式存储请求头（注意脱敏 Authorization/Cookie 等）

  -- 时间戳
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- 记录创建时间
);

-- 1. 按时间排序：几乎所有的日志查询都会用到时间范围
CREATE INDEX IF NOT EXISTS idx_visit_logs_created_at ON user_visit_logs (created_at DESC);

-- 2. 按用户追踪：查询特定用户的所有登录行为
CREATE INDEX IF NOT EXISTS idx_visit_logs_user_id ON user_visit_logs (user_id) WHERE user_id IS NOT NULL;

-- 3. 安全审计：快速定位特定IP地址的活动记录（支持网段/范围）
CREATE INDEX IF NOT EXISTS idx_visit_logs_ip ON user_visit_logs USING GIST (ip_address inet_ops);

-- 4. 来源分析：统计哪些外部链接带来的流量最多
CREATE INDEX IF NOT EXISTS idx_visit_logs_referer ON user_visit_logs (referer_url) WHERE referer_url IS NOT NULL;
