# Gemini + 粤语 TTS 集成（market-turnover）

## 能力
- 将输入文字改写为“香港财经播报口吻”（Gemini CLI）
- 输出粤语语音 MP3（Google TTS: `tl=yue`）

## 脚本
- `scripts/gemini_tts_broadcast.py`

## 用法
```bash
cd /opt/repo/market-turnover
python scripts/gemini_tts_broadcast.py \
  --text "今日市场整体表现偏强，主要指数普遍上涨，且伴随成交量放大。" \
  --output /tmp/mt_broadcast.mp3
```

不经过 Gemini 改写（直接按原文转语音）：
```bash
python scripts/gemini_tts_broadcast.py \
  --text "今日市场整体表现偏强..." \
  --no-rewrite \
  --output /tmp/mt_raw.mp3
```

## 前置条件
1. `gemini` CLI 已安装并已登录（若使用改写功能）
2. 运行环境可访问 `translate.google.com`

## 输出
脚本会输出：
- `TEXT_USED`：实际用于合成的文案
- `OUTPUT`：音频路径
- `SIZE`：文件字节数
