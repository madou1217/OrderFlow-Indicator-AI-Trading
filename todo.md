journalctl --namespace=orderflow -u orderflow-indicator-engine -f


journalctl --namespace=orderflow -u orderflow-market-data-ingestor -f

journalctl --namespace=orderflow -u orderflow-llm -f


下一个问题：你看下llm服务stage1 的输入里，avwap的指标有没有1d,3d的窗口？is ready是true还是false？7d还差多少？

