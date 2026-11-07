"""Render a captured stdout session as a five-minute terminal video; never invent command output."""

import argparse
import json
import re
import subprocess
import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--cast", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
p.add_argument("--font", type=Path, required=True)
a = p.parse_args()
if a.output.exists():
    raise FileExistsError(a.output)
rows = [json.loads(x) for x in a.cast.read_text().splitlines()][1:]
if not rows:
    raise ValueError("empty capture")
if rows[-1][0] > 300:
    raise ValueError(
        "recording exceeded five-minute render; preserve and inspect rather than silently truncate"
    )
font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 18)
cn = ImageFont.truetype(str(a.font), 22)
head = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 23)
command = [
    "ffmpeg",
    "-hide_banner",
    "-loglevel",
    "error",
    "-f",
    "rawvideo",
    "-pixel_format",
    "rgb24",
    "-video_size",
    "1600x1000",
    "-framerate",
    "2",
    "-i",
    "-",
    "-an",
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "22",
    "-threads",
    "2",
    "-pix_fmt",
    "yuv420p",
    "-movflags",
    "+faststart",
    str(a.output),
]
proc = subprocess.Popen(command, stdin=subprocess.PIPE)
lines = []
index = 0
try:
    for frame in range(600):
        t = frame / 2
        while index < len(rows) and rows[index][0] <= t:
            text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", rows[index][2]).replace("\r", "")
            for line in text.splitlines():
                lines.extend(
                    textwrap.wrap(line, width=132, replace_whitespace=False, drop_whitespace=False)
                    or [""]
                )
            index += 1
        im = Image.new("RGB", (1600, 1000), "#101923")
        d = ImageDraw.Draw(im)
        d.rounded_rectangle((28, 22, 1572, 86), radius=10, fill="#203345")
        d.text(
            (50, 40),
            "AIOps Research | Real captured session | Synthetic services",
            font=head,
            fill="#e1edf8",
        )
        d.text((1455, 42), f"{int(t) // 60:02d}:{int(t) % 60:02d}", font=head, fill="#77d8ca")
        chapters = [
            (0, "本地演示：合成业务服务，真实 HTTP 连接"),
            (30, "实时故障控制、告警与事件关联；不是 Chaos Mesh 实验"),
            (120, "真实 0.5B 推理与证据核对；模型结果和规则结果分开"),
            (180, "清除合成故障，持续采集健康请求"),
            (210, "观察实际恢复状态与不可变报告版本"),
            (270, "读取已保存主结果，并说明未通过的验收项"),
        ]
        caption = next(label for begin, label in reversed(chapters) if t >= begin)
        d.text((48, 105), caption, font=cn, fill="#77d8ca")
        for i, line in enumerate(lines[-27:]):
            d.text((48, 151 + i * 28), line, font=font, fill="#d2e1ed")
        d.line((40, 950, 1560, 950), fill="#334d62", width=2)
        d.text(
            (48, 965),
            "Local HTTP + real GGUF call | Not a Kubernetes / Chaos Mesh experiment",
            font=font,
            fill="#97adc0",
        )
        proc.stdin.write(im.tobytes())
finally:
    proc.stdin.close()
code = proc.wait()
if code:
    raise RuntimeError(f"ffmpeg failed: {code}")
