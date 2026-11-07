# 发布说明

本压缩包用于公开发布，已做如下处理：

- 删除缓存与本地产物：`__pycache__`、`.ruff_cache`、`.pytest_cache`、`.mypy_cache`、`.venv`、`node_modules`、`dist`、`build`；
- 实验记录中的个人 API 端点已脱敏为 `https://<redacted-api-endpoint>/v1`（涉及 1 个文件）；
- 仓库内不含任何 API Key；运行时通过环境变量注入（见 README 的 LLM 配置章节）。

被脱敏的文件：
- `research\results\p1-api-deepseek-01\result.json`
