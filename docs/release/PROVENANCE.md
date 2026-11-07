# 来源与复现边界

原始 phase10 包保持原样，完整历史产物仍在用户提供的文件中：
[aiops-platform-mvp-phase10-fixed(1)(2).zip](https://chatgpt.com/library?file_id=libfile_f7eec5a11ebc8191ab154db8632dad6e&account_id=personal)。包摘要见 `docs/research/original-archive.json`。历史 artifacts 没有删除或覆盖，也没有重新打标签当本次实验。

原始代码导入 commit 为 64195e2。所有后续 commit 均在本次改造会话产生，使用真实时间；没有伪造早期开发史。基线与最终测试必须分别看对应 result/log。旧说明留在 docs/historical 与原 phase 目录，其历史指标不能代表当前外部有效性。

P1 真实模型实验使用冻结 commit 9abacc9cf741f736dbc6564e90275c2fb3b08621；不要用后续源文件哈希去误判其原始记录。0.5B 和 1.5B 使用同一冻结协议，各有24 dev、36选择式test、36预注册强制分类对照、30攻击前后两条件。test 不作提示调优。人工复核未完成。

独立审计发现原注入评分遗漏“合法但错误的仅 e1”目标；原始输出、result 和旧图保持不变。新 `correct_attack_metrics` 对保存输出作明确标注的事后指标修正，未调用模型，未修改 prompt/阈值。主报告/四联图采用 correction-02 的指标，不能把原结果中的原始 ASR 当完整攻击成功率。修正同样应用于两个模型，不依结果选择攻击。

延迟受共享主机负载影响。推理期间存在工程测试及工具安装活动；科学计算/模型库版本未替换，相关新增工具不参与推理。时延不是隔离硬件上的模型规模因果实验，不做显著速度优劣宣称。token 单价未配置，成本为 null。

所有实验先在本机临时磁盘完成并关闭文件，再按字节核对后归档。P0 最初两轮完整指标与得分相同，但镜像目录的 live 日志缺行；原文件保留，权威 run 为第03轮完整日志。来源及原因边界见 research/results/RESULTS.md。独立验证 helper 是派生核验；新的 verify_results 提供完整实验契约的重算记录。

新结果中的 BLOCKED、partial、null 和失败记录具有意义，不能被简化为“全部通过”。kind 三节点、真实30次故障、三档10分钟真实EPS、人工标注复核、远程CI、Docker和干净容器demo，均需各自的实测证据。

## 最终源码版本映射

- 初版质量记录 p2-quality-01：a3ead82；原生验收 final-acceptance-01：50247d7。这两版不能混作同一冻结源码。
- 审计修复质量 p2-quality-final-02：64a191a；集群预检02也在这一不可变快照运行。
- 最终服务器入口修正后，p2-quality-final-03、final-acceptance-02、p4-demo-clean-312-03：fe8df8ac663c375e18f608bebcbcf01c27b4ef7b。后续提交只有证据、文档和单独演示材料，不能把它们描述成重新运行研究模型。
- 两模型正式评估始终为9abacc9；两模型correction-02和组合图是新文件中的派生计分，不覆盖原始推理。原模型输出均实际来自GGUF。

模型权重与公共原始数据未随包复制。P0保留得分/标签/预测NPZ，P1保留全部原文、映射和审计字段；评分和图表可以在不重新请求模型的情况下复核。最终git bundle记录本次真实提交，未伪造原项目历史。
