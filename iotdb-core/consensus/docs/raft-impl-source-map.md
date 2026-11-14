<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->



# raft-impl source correspondence

Functional source: `374f166686f5ce8b7ee161289b31d08f1b09323e`. Upstream base: `6de79acdbe7f0576eaf57bf218e7162ea5e37031`.

The table records each changed source file and its source Git blob when present.
Deletion of the old upstream TRaft is intentional. Runtime extension hooks replace
concrete LSM dependencies; protocol registration includes only implementations
available in this branch. Upstream integration changes are applied as three-way
patches rather than whole-file replacements. Formatting can change blob identity.

Later coalescing experiments, benchmark outputs, local cluster inventories,
manuscript material, and performance-only harnesses are excluded. The original
all-protocol integration harness is replaced by a protocol-selectable publication
smoke test. Sibling-dependent unit tests belong to their protocol branch.

| Path | Source blob | Publication commit |
| --- | --- | --- |
| `integration-test/src/main/java/org/apache/iotdb/it/env/cluster/EnvUtils.java` | `01741498b4cdc44e8bc13feb38f8f7b5f69c9bf9` | `b2215f2f68` |
| `integration-test/src/main/java/org/apache/iotdb/it/env/cluster/node/AbstractNodeWrapper.java` | `dce61d6a83a7644b478ed643d0ee030451e7f1c6` | `b2215f2f68` |
| `integration-test/src/test/java/org/apache/iotdb/db/it/consensus/IoTDBProtocolPublicationIT.java` | `New publication file or removed upstream file` | `b2215f2f68` |
| `iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/client/async/handlers/heartbeat/DataNodeHeartbeatHandler.java` | `80153685a93a2e92d92735da82050abbac3efb09` | `b2215f2f68` |
| `iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/conf/ConfigNodeConfig.java` | `c16eb33b70ebedfbd4e422ec7ff58eb620972224` | `b2215f2f68` |
| `iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/conf/ConfigNodeStartupCheck.java` | `8f002786b416d6b25fb5502fb0878e42027217b0` | `b2215f2f68` |
| `iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/ConfigManager.java` | `5cdeb03347b6776b3ef1f58ae8723f13eca526ea` | `b2215f2f68` |
| `iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/RouteBalancer.java` | `6985a998bc027d21444287487e08be1babaece85` | `b2215f2f68` |
| `iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/procedure/env/RegionMaintainHandler.java` | `6e46ab4b0b8d5c26cfaa9377be47ed3d70d2659e` | `b2215f2f68` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/ConsensusFactory.java` | `708d410f7b9839aa51c3e70e6e7b4e88fb36e586` | `b2215f2f68` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/IStateMachine.java` | `dd97aad501e81b391ea210045dd460e7f5f8617e` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/request/ConsensusWriteNormalizer.java` | `e45dcd0789362cf82b35953c3b8329c7c94eb11c` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/request/IConsensusRequest.java` | `cae54c2bcbcbc0b6fc8105f75bf084596b68c4af` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/request/NormalizedRequest.java` | `292c8566df991099c728a76945866f21a1d5fefa` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/request/RequestNormalizationProvider.java` | `ab24b6669539c536e3191f2f8fee2a94ceafe49e` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/request/RequestSemantics.java` | `2d135b823b4741603aee885f4e2cf2a4e6d76807` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/request/RequestSemanticsProvider.java` | `071c265e0e3ecb7f0eb7f200747667b5576156e7` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/request/StateMachineApplyResult.java` | `64dda9e16b8a55cffd96664d5fcd60238eb51141` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/research/ResearchIo.java` | `3c92bb2170137c6c3ccb0b5afe2e92b0dceff0b0` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/research/ResearchMetrics.java` | `09272850341408d6cc6803abd3aca17e8cfc1840` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/research/ResearchMetricsMBean.java` | `5212ff1678d383d837abad565c09b3b720a10b9a` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/common/research/ResearchReplicationLink.java` | `a406ab51b5ec3daa4fa0ed9e06b08d79dd05d73f` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/config/ConsensusConfig.java` | `dba53214abd7ea7cc8783509569e4b46b26af167` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/config/TRaftConfig.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/RaftConsensus.java` | `8ae0a4432b2932cd32a8b689dca082a3139b2537` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/client/AsyncRaftServiceClient.java` | `c49c05a8917b15bd01765fd37ec70946fdeba102` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/client/RaftClientPool.java` | `7f1abfb9f081387e86b803facb777e4bd9da5dfa` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/client/SyncRaftServiceClient.java` | `d4473cda0c729fca9ae29bd8bc421126b8f337dc` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/config/RaftConfig.java` | `d2baa5f6fdd97bb12630683bb69b1b8c76e608be` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/config/RaftConfigLoader.java` | `5534dcc4a2762939e7e08f32b42b847533108306` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/AppendReceiver.java` | `03d59a85a31049def8cad1755b7b82d11addc103` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/CrashPoint.java` | `300bd15fa4aebbde1aa184c767f260852499cab4` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/CrashSignal.java` | `f92a3d22773a3083ec468ee12b4d6ed350a85b15` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/ElectionModule.java` | `53005935bc221b92ff5aa23a03f45f3b72ba2cad` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/FaultInjector.java` | `52d9b2dc49a0132a4761a16f510fe57d41be41ab` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/Invariants.java` | `2e77905a78891f054991c5e0995ef5f91a3135f4` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/Membership.java` | `a41ca933b85a451a31fc7cef71c20b5211d4c51b` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/MetaToken.java` | `362ee1b4047296204f65b223887c7830c910eaaa` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/NodeEnv.java` | `261ecadfc326349a45d80fbdaa8271a2250b207f` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/PersistToken.java` | `6c1ac4c509cfb4e727cbc17f6f3a5e02e8e61e18` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/RaftCore.java` | `1c6567f4911fda90808d1314c005a9a63484acc0` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/RaftCoreConfig.java` | `1d46bf3674683832f474f0c5bc828bec1b456b41` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/RaftState.java` | `8d43284134be46b240042a8cad4bf076994a6371` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/RandomSource.java` | `57e788226747817b3fe725efe8bee198c34a3753` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/Role.java` | `051b70751fce0eb1eaddbbbb91ff905a6abd5fb6` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/action/Action.java` | `8a207156e942131baa1788bef3eb71e2f890d5fa` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/action/SendMeta.java` | `7ca42d9144ea60ae6eb3957b257fd40bd6e7005d` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/apply/ApplyContext.java` | `41950ec112aa5c6928fb34594098b89da11a3886` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/apply/ApplyPolicy.java` | `53bfc000ce0d7b9bda27c14d2326aa7e0027ad21` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/commit/CommitDecorator.java` | `34ab371725bef7952b6266081032c393db01561f` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/commit/CommitPolicy.java` | `2efe9601f11bbb74992cdd4f15cd9c0aa69c9691` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/commit/ContiguousQuorumCommitPolicy.java` | `761c47f2f5475aa90457eb9cbf512f298b102682` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/election/RaftVoteKeyPolicy.java` | `cc198ed85d9edd4cd2bdf48dc62403f28d04c62d` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/election/VoteKeyPolicy.java` | `315fabb9f93f8c4f854ca870209301f57e7d44ed` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/event/Event.java` | `2c41b9884513cc739259bef42e650d239ffd82b4` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/ingest/EntryFactory.java` | `bfb9b6084d3354f2b88aa94c764093a87934cd9f` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/ingest/LeaderIngestState.java` | `40d71cee554c9d03df2e811d201a9581be86ff89` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/ingest/LeaderTransition.java` | `724be4e04bca35ea9827ca2b5afedd558a45957c` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/ingest/RaftEntryFactory.java` | `e2fb0071fc2c5a4561f971618d0b1591e7153ea1` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/ingest/RaftLeaderTransition.java` | `c5a1ac8c2c873679edd89f942df95b9c173bc823` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/ingest/RequestMeta.java` | `67be44b92e469d6e803ac3cc3dde9c5f18e7e91a` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/listener/ListenerEvent.java` | `0888a26c9047cd1bc7c4211ed21326aab953eb3d` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/listener/ProtocolListener.java` | `b09655f55cb8c27755a416f934f449fc7d57f8ce` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/ChainValidator.java` | `8ee40e027565aa08c14178ad03cc723aa645450e` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/EntryMeta.java` | `909c4dd8271d7030e97fb1e4d9f4332753be0b56` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/EntryMetaCodec.java` | `cbb73fe9f043878a3c6d48b7b1d09c225def29c7` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/EntryType.java` | `4baabd5ff12fde9e24e67ee50f0b2955602ae720` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/LogEntry.java` | `18437d7b257ca7e46ec19f7378eade4f2c291564` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/LogIndex.java` | `ed7388d39f57207e596463f4c52f201da9f5dc78` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/LogView.java` | `9df5fbfc3725cdd23c1533151098927db782e707` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/MembershipCodec.java` | `167fedcdc31125797251e5a7c9149f2317f7e46a` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/PartitionedMeta.java` | `7d5b5aaccedeffd7380ca4b3459bcad91adb34aa` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/log/VoteKey.java` | `0f55d19641f39943b6e6f3c0c6bce2f8528c5589` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/AckStatus.java` | `100c7c51b65e6e960a8367fbb6fdb1104e3b3ee1` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/AppendEntriesReq.java` | `6977d7940d614b224630b85a3c1d78e1470d8dd6` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/AppendEntriesRes.java` | `30d66c1904ce20ca6a2317e4fae3ea2000d51bf9` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/AppendExt.java` | `40ea8d488401d19a8bd5c13f289f3267198dc30a` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/EntryAck.java` | `1e6ccfc99313556293b7f7aeae4fbfcbc9d3be10` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/Message.java` | `cb905299caf5d3764cac21e008cdd2e471fceb3d` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/RecoverTermReq.java` | `e93b692af9e9d3838678316d4d73d2856112c55f` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/RecoverTermRes.java` | `a3176cdb07ac0b4d88c3a4217354fa7472dbf4ca` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/ReplyHandle.java` | `bb4f21ce22442c5e4a7c5a010734193f767d72c6` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/RequestVoteReq.java` | `fd191b15aee1cd1655bde5e4277c61c1a8957213` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/RequestVoteRes.java` | `1aa2a5da7f450fce5c2f756856e10d4d5107b72a` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/TimeoutNowReq.java` | `dbb8ffc3c2270e19451cfc412eec4f31698c6a37` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/UpdateSyncReq.java` | `eaea56720d16034946c895e471abf1f710b9e025` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/msg/UpdateSyncRes.java` | `a0ee85ca3bf4a2e18d6cc2df80ff0747c0fd5b3d` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/policy/FidelityProfile.java` | `c250d8b48debf768ab36a7655ac59ae1e72fb071` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/policy/Mutations.java` | `b547ec0cf076bc40e3c6f22dc0ab3cb3d84b818d` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/policy/ProtocolPolicy.java` | `8298a5b2b58615f0b0deac3aa59d5afacef1fbf0` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/profile/Capabilities.java` | `07efbb9a15445073f4594a2e73d41f658d4c4326` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/profile/ProtocolProfile.java` | `f3c90676470bffaa894a9f3aff350bebad595355` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/profile/RaftProfile.java` | `3fea8f395dc22a128b4a8080fcd3ee87c24a9358` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/profile/SnapshotMetaExt.java` | `3b694653e2a6afcf035a8798a14f614fb6763015` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/AcceptGate.java` | `dafaf124a929b8daf460b0c856fa533dfdb26aa8` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/FollowerProgress.java` | `9d50370b093c81d97b0cafc1c791fd90dd3407d8` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/LeaderStateView.java` | `42d32939700a7bde349c72ad5e38f6470c5c4475` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/PipelinedReplicator.java` | `6290ee0cf9797c2b45103345d2a46e82de8beed3` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/ProtocolContext.java` | `c655a6701dcb28129e4bf65c630fe3913b97d697` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/ProtocolRequestHandler.java` | `c072326095569f023a84a0bbb1bf6e46e5934575` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/RaftAcceptGate.java` | `e3179c08e61daa361ca227aed9c033d6c3af35bb` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/Replicator.java` | `8cd29e51786ad699b6456e15e8497b6ec604bf9f` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/core/replication/SchedulerContext.java` | `a2e1e11d763a4571dd9688a944611c05aa8ed3f1` | `9abad37d1c` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/ActionDispatcher.java` | `186a894d77e70a177ac30f33b92c9e25101a3929` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/ClientRequestTable.java` | `16210f9ac2563908b559fc827f24dc368b1db8b7` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/GroupExecutor.java` | `b7c7921190beb269b180dd8771f3a7c50a773c39` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/InboundFaults.java` | `baf49d0dc2bd415d996c3b33b2d5034a8581491f` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/NotLeaderException.java` | `46634bd00f17bac6f1c998c7e6a676807cbd3511` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/RaftApplier.java` | `5eb0b4f9a952be8587e3552d8ce641afc05521a2` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/RaftGroupMetrics.java` | `fda8d25d1b0293ab088d36e01cc1b5f2ee323e00` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/RaftGroupServer.java` | `3bd614d2403936d8bff8734069928ddc93f109af` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/RaftTimer.java` | `68deecb347dc3021995c70d5a239ba3c066567c1` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/RequestPreprocessor.java` | `6bb3cd81a0f54e671b16b15e5bec46d5b273b872` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/server/WriteForwarder.java` | `711a0ff6f3b29c22ee4e815cbc10959b66b0fa2e` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/service/RaftRPCService.java` | `0815fb5be0762a05215689652a9956a721b9a97f` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/service/RaftRPCServiceHandler.java` | `22e98b02f09943d314dbaa39868236d61ba95c85` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/service/RaftRPCServiceProcessor.java` | `8a7721a8d14036127a7742ea4806491161f23fa7` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/service/RaftRpcDispatcher.java` | `0cd1add7a640656f83553769327b49f04e9d13d8` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/snapshot/SnapshotBundle.java` | `074280b8918c9090d14345881ffadfaf92c8f55f` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/snapshot/SnapshotManager.java` | `ebf6e491f111663da58e6fa445a79766523e927f` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/snapshot/SnapshotMeta.java` | `79595e40f0e3d67067d0313543adfdfe4fb53c6f` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/AppliedCheckpoint.java` | `4e7678ffc5834fdbcb313018edbf52ec34b4bf9b` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/AtomicFiles.java` | `dab542a6caf11e0a0c41bd038e857a1310fc24a4` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/EntryCache.java` | `b8151753cf5b622d3e3a27755561aca9f752c0b8` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/FileMetaStore.java` | `2364e2e45d9469cef2b278107a27d39cfa008c99` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/LogMeta.java` | `c9d7a4cef9056aed38ef43fa8c0eed1c932c52f7` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/LogRecordCodec.java` | `9a6582c41f99d846564391adadc8a16ee14de58c` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/LogStore.java` | `53df1c4b0722c3a0a4d0027035856da45a055f25` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/LogWriter.java` | `8a9f3e8a6801df6c7ef6752f29993071d97da378` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/MembershipStore.java` | `aa602cafcf070d644e7b485c1edff589daeb07c4` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/MetaStore.java` | `6132221adc47b1d94c029ecb69da1b3d2a52f2c1` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/PersistentMeta.java` | `f21a48548087606aabe5361a0c40cea62f6c92c1` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/storage/SegmentedLogStore.java` | `6f945d12d6e61fcf9c4e3c5d9996ad295c707581` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/transport/LogFileTransfer.java` | `New publication file or removed upstream file` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/transport/ThriftCodec.java` | `b6a8404dac353f9aa717ecb6c44a1be0caa9b235` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/transport/ThriftTransport.java` | `7d06186913e8c8fa50f2df954801b808f9fdd81d` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/raft/transport/Transport.java` | `848612486381bd8076478aceaf1c5344107013e1` | `823575b139` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftConsensus.java` | `0e956ab4a2f0bbb4f3bce9c2f61c92e8794b52fc` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftFollowerInfo.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftLogAppender.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftLogEntry.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftLogStore.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftNodeRegistry.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftRequestParser.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/main/java/org/apache/iotdb/consensus/traft/TRaftServerImpl.java` | `New publication file or removed upstream file` | `7dfc4d8c46` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/DurableOfAReplacedCopyTest.java` | `692352bf3e4846a6e6b4b771303d37876bb022ee` | `9abad37d1c` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/MembershipTest.java` | `95fdfdebf475cf7f9bf087495b70e755f22a1aec` | `9abad37d1c` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/RaftDurabilitySafetyTest.java` | `8ef0201f191adbbdc9cad7fe892a51aa263db217` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/StubOooProfile.java` | `a620452909fc44b1fc4a842004a219a9eddbf193` | `9abad37d1c` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/commit/ContiguousQuorumCommitPolicyTest.java` | `ec01268e024e67f75db00d7b952d775b8b5ab2c4` | `9abad37d1c` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/log/LogIndexTest.java` | `8add9c29bbf8dcc09f19b721d031310eafc77738` | `9abad37d1c` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/log/VoteKeyTest.java` | `1e26bd73800c539c5f420eb999dbd03f6cfd3f38` | `9abad37d1c` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/core/msg/MessageExtensionsTest.java` | `cf810c283724677aaa0beed2b0176b174dd2b2cb` | `9abad37d1c` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/ActionDispatcherTest.java` | `b4805200cb41a52fb9d61683f38667babddc575f` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/ClientRequestCacheEvictionTest.java` | `5b91d788f3181efecb508affe03bc9a7e5669770` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/ClientRequestTableTest.java` | `a20ca46180e888c31afd8bfe3a233eba65a809e8` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/LsmClientRequestTableCompatibilityTest.java` | `c736c1b45b0def2f0288ac097c17084b25e58eab` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftApplierCacheEvictionTest.java` | `9ad2c152daaf861f3ce0b1ec2086e93869dc344e` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftApplierTest.java` | `462e51653513ef98144851c79482d973ad5ac5ed` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftConsensusCacheEvictionTest.java` | `60cf4d4fd5d94e56aabbd54f06ed01fefae0ea5d` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftConsensusMembershipTest.java` | `db7bc4ac97e12ffe8c3004e2c3bf712495afaffa` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftConsensusThreeNodeTest.java` | `5a4dc92abd4c76a17943b3f68c805cf0d3334a7b` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftConsensusWaitCancellationTest.java` | `d5c090f3c726b28af3e2592ea3cc615606c896b9` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftGroupMetricsTest.java` | `6b84c70f1d6a058f13bcfc339637827b8f5feee1` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RaftReadConsistencyTest.java` | `90ffce453d435eb0f27a445716ea2752868c6451` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/server/RecordingStateMachine.java` | `45ace09e1f294442eec3e6acd3bcfb7817707a6c` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/snapshot/SnapshotBundleTest.java` | `ad647e24b992639fd1083a785616248db05c4b89` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/storage/LogRecordCodecTest.java` | `da696a1a351fd27dc98174b897b82363f53e9564` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/storage/LogWriterTest.java` | `dbc81ea16a638e4eaa71b4b10b107b9db36b98e8` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/storage/MetaFilesTest.java` | `df5cd5f4f466ba1f1afbf1c2f4a622c9ebc41957` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/storage/SegmentedLogStoreTest.java` | `517d295e56b762293ae144126f9c538e8079b394` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/transport/DatedTestMeta.java` | `d67c050e2c006103ca860058161cf390dddfd785` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/transport/RaftRpcLoopbackTest.java` | `6f82ae44d7710816713d76ee966d66a8d7666384` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/transport/RecoveryRpcLoopbackTest.java` | `985cd4a7ec06cc7d22fe88df5e136565b11d80c9` | `823575b139` |
| `iotdb-core/consensus/src/test/java/org/apache/iotdb/consensus/raft/transport/ThriftCodecTest.java` | `2c42720c0b0db747aadbc5367a447ae4bfc432a1` | `823575b139` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/conf/IoTDBStartCheck.java` | `e5ba2f2b5a27523c1b9493c3fd5deac9e78329ed` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/consensus/DataRegionConsensusImpl.java` | `f70a10d2df5f4cec84684a85c5a19101252d5aa0` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/consensus/statemachine/dataregion/DataRegionStateMachine.java` | `e2a5484d9833223d8516028ec3267b4dc12fb8d0` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/consensus/statemachine/dataregion/ResearchRaftDataRegionStateMachine.java` | `50d06657d86215d20a620659206eddfb6ab531b1` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/source/dataregion/IoTDBDataRegionSource.java` | `915fb353b4a4c3bdc523963757c69cb691027832` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/protocol/thrift/impl/DataNodeInternalRPCServiceImpl.java` | `6fcf6cfe94fdc2f5874229ece57c7c6150125085` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/pipe/PipeEnrichedInsertNode.java` | `2e517700217b764aaba7aadde2bfb01a8cb5005d` | `7dfc4d8c46` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/InsertMultiTabletsNode.java` | `f62d22dbf735e3841056f51f221940d5de040639` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/InsertNode.java` | `35d5d34ec17fa8b816fbeaf3bfccb48729ec58bf` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/InsertRowNode.java` | `4aae5a7b0a00aeb27011c174c9e18e7922a7d0b2` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/InsertRowsNode.java` | `16877a0af1f91c6caddb3b99f16189e639b94f88` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/InsertRowsOfOneDeviceNode.java` | `39a9d0f36e537c2d76cdc9a60dcb3c645e49bc30` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/InsertTabletNode.java` | `05ad0d4cf72449c5b08633ee6316033f47380d22` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/RelationalInsertRowNode.java` | `08c783287328cd8c2bfa11940a4a8dc0bd9c3df5` | `7dfc4d8c46` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/RelationalInsertRowsNode.java` | `594ccf50471f92ad6f1286d3994cd8924df73b9b` | `7dfc4d8c46` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/RelationalInsertTabletNode.java` | `257f691e4a78570e1975c0d3fa0c4abb87968ede` | `7dfc4d8c46` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/plan/node/write/ResearchInsertNormalizer.java` | `044b6da60a4d045ce1def2a7c34045923f3b70a6` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/service/DataNode.java` | `1dc8e72708ef876aa7d56634b41a8f16d9493de5` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/service/DataNodeShutdownHook.java` | `765027c68982971370b2fdcc1ef70e43351ff341` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/storageengine/StorageEngine.java` | `98cceed6a3f8fabf548acd4f656085d27a842423` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/storageengine/dataregion/DataRegion.java` | `c27ec6de3dfa3f47b6de615cc54685b1bddc68ad` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/storageengine/dataregion/memtable/TsFileProcessor.java` | `5449944c056b223e8ca09955ad13108f856ab8a2` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/storageengine/dataregion/snapshot/SnapshotTaker.java` | `14db3776c75e9e3ff9fd22a968c86cdb89ae8a1c` | `b2215f2f68` |
| `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/storageengine/dataregion/tsfile/TsFileResource.java` | `d41e14201135f3db3da13a85de37e4a63588c614` | `b2215f2f68` |
| `iotdb-core/datanode/src/test/java/org/apache/iotdb/db/consensus/ResearchRequestNormalizationTest.java` | `326c45610651dd1037ed1c683b3be78f96672c3d` | `b2215f2f68` |
| `iotdb-core/datanode/src/test/java/org/apache/iotdb/db/consensus/statemachine/dataregion/PortableDataRegionSnapshotTest.java` | `10ca11b86caab49e09e9098bfacc99143b4cbd8a` | `95560045e8` |
| `iotdb-core/datanode/src/test/java/org/apache/iotdb/db/consensus/statemachine/dataregion/ResearchSnapshotConcurrentCloseTest.java` | `95a64dd1d96c99bb5cf4b348a07ce738064d322d` | `95560045e8` |
| `iotdb-core/datanode/src/test/java/org/apache/iotdb/db/consensus/statemachine/dataregion/ResearchStateMachineWriteBatchTest.java` | `b6fa540a1d810f25c42edc89e13f686deb0f7574` | `b2215f2f68` |
| `iotdb-core/datanode/src/test/java/org/apache/iotdb/db/storageengine/dataregion/snapshot/IoTDBSnapshotTest.java` | `ebaa80a95975ed1fbbbd17d6d3616c0f1bbbb187` | `b2215f2f68` |
| `iotdb-core/datanode/src/test/java/org/apache/iotdb/db/storageengine/dataregion/snapshot/SnapshotTakerConcurrentCloseTest.java` | `f706d0ad26a675ecb70667a11b5cc467925b6050` | `95560045e8` |
| `iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/consensus/ConsensusProtocolUtils.java` | `c0689fb86374afc01f90b770445c607bac2c13f8` | `b2215f2f68` |
| `iotdb-core/node-commons/src/test/java/org/apache/iotdb/commons/ConsensusProtocolUtilsTest.java` | `fe1285c8e4ee79f1fcdc57fc0a5242c3bf6128a9` | `b2215f2f68` |
| `iotdb-protocol/thrift-consensus/src/main/thrift/raftconsensus.thrift` | `ca625b80c3404f03c9048e3801ed1f3f2e470b76` | `823575b139` |
