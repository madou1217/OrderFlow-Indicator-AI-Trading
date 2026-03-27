
stage1 scan的目的是解析市场和解析主要参与者的目的和行为

stage2 core entry的目的是llm阅读数据后清晰的准确的给出交易的方向，tp,sl,entry，leverage


stage2 pending管理可以让模型准确的定义近15分钟的风险以及给出最优的挂单的方向，tp,sl,entry数据？

stage2 的management管理可以让模型准确的识别4h,1d的原有趋势是否延续，并关注15m的风险，能够对持仓的机会和风险做出很好的管理





你写一个/data/docs/llm层工作流的修改方案v1.0.0.md 把现在的问题和你的解决方案都写进去，注意stage1 scan的目的是解析市场和解析主要参与者的目的和行为，从第一性原理出发去思考，找到本源问题，我觉得根本问题是 stage1 scan的本质工作就是解构市场和当前参与者的目的和行为。他本身是不是就不应该带预测的字段，比如高时框标签太粘(sellers始终active)

