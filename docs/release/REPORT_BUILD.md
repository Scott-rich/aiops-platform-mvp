# 重建报告与录像画面

报告生成依赖独立锁在本目录 `requirements-lock.txt`，不混入平台运行时或模型环境。本轮在全新的 Python3.12 文档环境安装并执行依赖一致性检查，再实际生成报告，日志保存在 qa/。字体来自固定 Google Fonts revision，许可与静态字体附在交付包根目录 report-assets/；原始来源与哈希见 font-source.json，实际嵌入字体哈希见 report-provenance.json。

在项目根目录执行：

```bash
python3 -m venv /tmp/aiops-report-env
/tmp/aiops-report-env/bin/python -m pip install -r docs/release/requirements-lock.txt
/tmp/aiops-report-env/bin/python -m pip check
/tmp/aiops-report-env/bin/python docs/release/build_report.py \
  --font ../report-assets/NotoSansSC-Regular.ttf
```

此命令重建展示文件，不改实验原始结果。完整结果已经随包提供，生成 PDF 不需要再次调用模型。视频渲染另需系统 ffmpeg、DejaVu 字体和已记录的 session.cast，命令见 demo/script.md。

已实际渲染全部报告页面并检查排版；统计主表、攻击对照与A4简介另作放大核查。最终报告为12页，简介为1页。最初发现的深底表头对比度问题已修复；归档的旧审计保留当时状态，不能当作最终PDF现状。
