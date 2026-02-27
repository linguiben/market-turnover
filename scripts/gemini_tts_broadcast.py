#!/usr/bin/env python3
"""
Generate Cantonese (Hong Kong style) voice broadcast for market-turnover.

Workflow:
1) (Optional) Use Gemini CLI to rewrite input text into HK Cantonese broadcast style.
2) Use Google Translate TTS endpoint (tl=yue) to synthesize MP3.

Examples:
  python scripts/gemini_tts_broadcast.py \
    --text "今日市场整体表现偏强，主要指数普遍上涨。" \
    --output /tmp/mt_broadcast.mp3

  python scripts/gemini_tts_broadcast.py \
    --text "..." --no-rewrite --output /tmp/raw.mp3
"""

import argparse
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path


def run_gemini_rewrite(text: str) -> str:
    prompt = (
        "请将以下内容改写为香港财经主播口吻的粤语播报稿，"
        "要求：口语化、自然、可加入少量中英夹杂（如 market/volume/support/resistance），"
        "长度80-140字，只输出播报稿本身，不要解释。\n\n"
        f"原文：{text}"
    )

    try:
        result = subprocess.run(
            ["gemini", prompt],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
        )
    except FileNotFoundError:
        raise RuntimeError("gemini CLI not found. Please install/auth first.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"gemini rewrite failed: {e.stderr.strip() or e.stdout.strip()}")

    rewritten = result.stdout.strip()
    if not rewritten:
        raise RuntimeError("gemini returned empty text")
    return rewritten


def synthesize_yue_mp3(text: str, output: Path) -> None:
    q = urllib.parse.quote(text)
    url = (
        "https://translate.google.com/translate_tts"
        f"?ie=UTF-8&client=tw-ob&tl=yue&q={q}"
    )

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(req, timeout=30) as r, open(output, "wb") as f:
        f.write(r.read())

    if output.stat().st_size == 0:
        raise RuntimeError("TTS output is empty")


def main() -> int:
    p = argparse.ArgumentParser(description="Gemini + Cantonese TTS generator")
    p.add_argument("--text", required=True, help="Source text")
    p.add_argument("--output", required=True, help="Output mp3 path")
    p.add_argument("--no-rewrite", action="store_true", help="Skip Gemini rewrite")
    args = p.parse_args()

    src_text = args.text.strip()
    if not src_text:
        print("ERROR: text is empty", file=sys.stderr)
        return 2

    final_text = src_text
    if not args.no_rewrite:
        final_text = run_gemini_rewrite(src_text)

    out = Path(args.output)
    synthesize_yue_mp3(final_text, out)

    print("TEXT_USED:", final_text)
    print("OUTPUT:", str(out))
    print("SIZE:", out.stat().st_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
