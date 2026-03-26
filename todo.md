通过sudo journalctl -u orderflow-llm -f查看日志，从上次重启到现在，帮我结合源数据 帮我分析一下，目前存在哪些问题，导致最后亏损的



请根据最新重启后stage2 core entry产出物: 20260316T121800Z_ETHUSDT_entry_core_20260316T122015301Z.json再次确认本次针对entry filter的优化实现了：llm阅读数据后清晰的准确的给出交易的方向，tp,sl,entry，leverage



是否可以让现在的stage2 的pending管理可以让模型准确的定义近15分钟的风险以及给出最优的挂单的方向，tp,sl,entry数据？

是否可以让现在的stage2 的management管理可以让模型准确的识别4h,1d的原有趋势是否延续，并关注15m的风险，能够对持仓的机会和风险做出很好的管理

stage1 scan的目的是解析市场和解析主要参与者的目的和行为





请结合日志和源数据20260326T083000Z_ETHUSDT_scan_20260326T083213703Z.json帮我分析一下，模型给出的结果看全了价格带了吗？有没有遗漏