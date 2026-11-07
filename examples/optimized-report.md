事件：6b8c4407fa0748bf9070d44110bfaa5d / 版本：4 / 阶段：resolved

# 诊断报告 ddf13356-1ba7-42fb-aee6-ff4389a7772a

- 故障事件：6b8c4407fa0748bf9070d44110bfaa5d
- 报告版本：4 / 阶段：resolved
- 告警：b5d5779cd31904bbeaa039f2
- 服务：payment / 规则：fast_errors
- 分析方式：heuristic
- 窗口：1792467498582–1792467499892（UTC epoch ms）
- 证据 SHA-256：`7a6ed4fe30800e168ff5c20eac4e5f3430be2fc3229602bffdcc3be57f7f5aa7`

## 分析

故障事件已达到连续健康确认条件。payment 触发 fast_errors。建议先验证 payment；此结论来自规则分析。

## 已观测事实

- gateway观测状态200,502
- inventory观测状态200
- order观测状态200,502
- payment观测状态200,503

### 候选原因：payment

payment 出现异常观测，可能影响调用方；现有证据不能确定内部根因

判断等级：low

支持证据：ba562446e621420cb343237d70d3cc94, 4d44890e500e4e1893d53a925623d82d, ced02695444348ffaa28f5bd031182f1, 00ab72f161224dd79096f6c299340684, 48aad68ac5f14cc7ad8562f393d53dcf

验证步骤：

- 按 trace_id 检查 payment 与调用方的起止时间和错误码
- 检查依赖可用性及近期变更，复测故障恢复后的延迟和错误率

## 覆盖范围与限制

```json
{
  "all_topics_complete": "unknown; independent Kafka topics have no shared watermark",
  "latest_service_metric_time": 1792467544670,
  "metric_time_reached_window_end": true,
  "range_candidate_count": 1824,
  "related": 1812,
  "scanned": 1824,
  "selected": 1800,
  "trace_coverage": {
    "complete_anomalous_trace_count": 89,
    "complete_observed_trace_count": 151,
    "complete_observed_trace_ids": [
      "003a7d29bb224bd8ba3b2d9b4c0403f8",
      "00a5db39d2f84101be9d3657aeac2384",
      "0267fe8c92da479fb8bd5f57c86ceb43",
      "03266e589d864e09abdc0878d6d11180",
      "0435b85fe99849399464a1829556d0a7",
      "04defe7183304d088d551a4736e05dd7",
      "062f96b11a9e4dae9feae3777daeaa79",
      "06e39c5d2ce54c6d9f2a7226463735a2",
      "078c611c635243fc87633b4f818400e7",
      "079918385b8545939e7b67d4b45e8056",
      "0ae85aefe66d49fd80ea8166d52b65e9",
      "0b8c3b34b98b406dbde8f6d5f2e570c5",
      "0c9a9992f9f342c1baf5d31f1b6d8a82",
      "0d7a080d62974a568e7ba8b3235b4443",
      "1002c42af3984e829681ae867dabf862",
      "101e37b888684e5ea4629d6e25a78194",
      "141cb732cc3247c6be0a12debbec1f56",
      "149b338058954f1eacc7f178b2f83a94",
      "183268fbceb24b15a78c91ce520417fb",
      "1aefc370fa5049bdb5091c4cee1fe4a3",
      "1fa41285824b4e3d9eab57b997ea43a0",
      "22f4d1428ca444f680d26360b6c52093",
      "23025a71347e4fbb89f2c830d00c6665",
      "29a0d89865524ec4a7078fe828c15db4",
      "29c74cd3c2cc4b29880d73cbf85ff942",
      "2e7d75f8377d44daa81a70221564a47f",
      "2fae7f3adae642d5a993ddb988b4dd35",
      "3175570a20c74cc4897fdaa506d677c3",
      "324f56b44bfe4262b3e544b91cd4ec2b",
      "36df7fdff3574293b9013e5613b3068d",
      "37923a5c33c0464589f8397c5e57a5fd",
      "3b60382094324453a3c60dffa5553780",
      "3bddc009b661469580999465e4b55d41",
      "3cee191975f3462a9ee0e52cb08fd4fa",
      "3fafa0a103404ba8a0b5c17c3b9bc3f8",
      "42761dd055354d1c8b601f1b8ab81e1e",
      "42da7e74c0874c0790d54c306c5a9449",
      "44e83ceea02b4d26ae15f6317e749eb8",
      "4541cdaf079f4d0dbf50d1ad0ba4cee4",
      "467d95b36984435f8d7fcb0f154e6340",
      "46801ef2a4b54ca79acde6a02888c839",
      "4874c36ce4f24b70b281472050ec5525",
      "49be5170a75f41fd8a6a7805b1676c5b",
      "4f599e879b8c4c24a9261c9628654d41",
      "512eac861cf44742a5d3a476b139ad18",
      "5207f8baaec24c008de689a204b56997",
      "563afb3b7a784390a36de5ca4eaf7192",
      "5b6c7e61466c47108fba2ca27e24b4f9",
      "5c642beed2974897b6f604f5e78fb327",
      "5e0414bcbb9c4c429ed55f725bebdcd6",
      "5efe0234d2154faa8f76be1e5f9d53ad",
      "5fb96bda7d394f2a842c591c27e9aa2f",
      "6000f6a5335d4529862c4b32667b6bcd",
      "6012512d0bab4935bbcfd0c6f611d411",
      "62cfb1760b234536b3935e500b580d2f",
      "647b2f7a23e04d7f8d4bf85703a42ef0",
      "6c7ab8a938c949149a49698c2f639977",
      "6d01a6ba36c4443aa2e9d9f84703ed26",
      "6d7d1266647345ef8393bb48bc48a6cd",
      "6dbe78fe57ab449f9bd33d6b3967ee07",
      "71c53b8b1ee549d3a7b6ebf58719d707",
      "72c8f79927c84d87bbda334db1ea737f",
      "7313128d5b8b40bda57f62f2e7d22268",
      "74d33761a84c4cffbf45cdc17c8eeb54",
      "7857c10cb0bc445dad3093c4423e434d",
      "7a20c5e8a30d4f7b8f4986367909db5d",
      "7a66a9788571456e9ed38b87e5c6a84d",
      "7b84be2f8e0e45f3b2929b7b1ff29c59",
      "7c3144f690d645f8b849b342f9c6b285",
      "7d285bcab918498c9e4fa703a229b814",
      "7e8d10d736ca4b498e77d42c77a74ec5",
      "81733f0993774e89a4ffac839090e25b",
      "817748efd4e54aaebdf89c38265c3d09",
      "831393d6312544b4a16560e72ea07be8",
      "8620b186db604b3db62bdcadfd8c4c66",
      "88ab1bb913a44d0b8c268b592e51a00d",
      "8cea5960eff9438e84258ec0e591a764",
      "8cf044db6f0b400dba7793f73fb9fce2",
      "8e1a3699720240c599f830e1b3d85876",
      "8ea228add27547d6a8ab218712d37ae8",
      "8fab377b1f3a47919f669354a6de912c",
      "91054fb49e7545ab878d8c30297b86d2",
      "91caa6359eba42a8a75636987b6a38d5",
      "9383e172c2bf4d19a52b1b878eab8fb4",
      "944e24c5be4c474faf1f72a4c025057a",
      "953d48bf1d0e4ade8c336726182ba5f2",
      "9aa1b54bcbed41dd825acff2d6116081",
      "9e61e052ae7c47a68653fd1ba6e63ccf",
      "a0690671464a4262a2cad352b8241b95",
      "a9653a527aed4f0fbe6bffdad1d49a78",
      "ab3fa02dbca7483a8a38587e17c79673",
      "ac5b35fd18cd46e7aa10b234abe4218b",
      "b525516ac16f4007bfaca9ad49882bb5",
      "b5ba10c7ae28465ea43ec49ff09b3018",
      "b7ab8cb270254157b080dcba84a697d2",
      "b81d01ec1e824d7c92e36b48009ede5b",
      "b84a428c85494d71843dd114ec68987f",
      "b8950cc5da0e47d3838a24c240f60bd1",
      "badbb13e13264d78866959a015df6385",
      "bd3c2e6711eb45079f63b78cafe275a1",
      "be7d751567f4492d9f4694c199a0ac3f",
      "be960147b80a4b0e8c6d0c7c21f8c608",
      "bfee16db440540cdb26426a45e342046",
      "c15089c234a14fdba62508150ba19d02",
      "c1953892441440c0acb2ba7c882e763f",
      "c280fb71aead4aa69f80fc46095ca9af",
      "c3000cb602be47f583f41c8282deb2f4",
      "c54770a6d0ba4710a19f9729c99966b1",
      "c550607fec4f415d92ce9fe6f6a5251d",
      "c763b1922f274761975040b1af187e24",
      "c8956599c82e45f597287d263653e4be",
      "c8a23fe5776047f9b0c02c9dfbacdea6",
      "c9fba16eb7374a919c2b918eb1b57f2e",
      "caf27eff69bf4102ba6ff8eb652a3bd9",
      "ce76f8abd4c44deab00b66099e2c8c1a",
      "cec281a5b6f047a0af19c898855cb2c8",
      "d0375a1707254312bb4482856e83007f",
      "d25086b55c8b4a12b2d9780b3d758ffc",
      "d265c045740947689338285d2c7cc54c",
      "d4f9e3434f1c4719a2bc3c8bc274c7ed",
      "d5e6c3f638d04a7ab2a6d8a82f56bae4",
      "d86a1ea034bd4335a0accdc1dd848eaf",
      "da42761b472241e9a4171b793a82f2ab",
      "db4836957c49422cadc63c5a6e001cdf",
      "e4122cbe63a0493c8d4961ff5492f3ad",
      "e41ccd98eb1940caba4baa009bdac269",
      "e787c3c08ad74cca8f7a76fa14cb6a04",
      "e792fab37c004ff99195fce879081510",
      "e80b7a427d2847a89074729ceadc2b29",
      "e840081a8926421392ab71bc2b96d953",
      "e9e39d7afb6445a88da694c39b965131",
      "ea4c3216f2f74120ad9aa88c0c92b4c7",
      "ea4fd34448544410aa75243504cfdb34",
      "ea82e861c19b4b5485ab41a8d0d8a82c",
      "ecb2f7d32c57493caab375c747d00110",
      "ed4bdbb1725c49c48346abecd1113ce8",
      "eeda8ca40fa2426898ca91af9e5710e4",
      "eef8dd34306443359cb12aac03eafea3",
      "f0083df4fe894369b1664d4409c907d6",
      "f084aecc620d4e51a43eba7e7f6a94a9",
      "f1475d683c704ba5ba47ae0b979cde74",
      "f1a173ee38f7465cacd19e2b10c3240c",
      "f2e6a217ae0d45fb9553079009020c92",
      "f3394b037d654a558c86a84d8c961453",
      "f486b4688faa425e830b56076b5871cd",
      "f5167aa49f584aa689b718aa2fa44f2b",
      "f5457366d46f408d80ff46837bb35031",
      "f55c149520924624b2b90ae444718068",
      "f5ffba0c066d4c7ab80ac7b1e8fc2155",
      "f72a519cb68242649d38af599d2aad23",
      "fe63d8924c2f43bf96a3523a872e4f3b"
    ],
    "definition": "Complete means the relationships declared in the observed records are present; unobserved services or events remain unknown.",
    "missing_downstream_span_count": 0,
    "missing_downstream_spans": [],
    "missing_parent_span_count": 0,
    "missing_parent_spans": [],
    "missing_request_log_count": 0,
    "missing_request_span_count": 0,
    "trace_count": 151
  },
  "truncated": true
}
```

- 候选服务和内部根因不同；仍需依赖、资源与变更证据验证
- 完整观测链不证明跨主题数据全部到齐
- confidence为判断等级，不是校准概率
- 报告版本不可变，补证生成新版本

完整原始证据、实际传输来源、历史案例和模型输入输出见同名 JSON 报告；Kafka坐标仅在实际经Kafka采集时存在。
