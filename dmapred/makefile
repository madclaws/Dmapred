start_master:
	iex --erl "-dmapred type master" --sname master@localhost -S mix

start_worker:
	iex --erl "-dmapred type $(WORKER)" --sname $(WORKER)@localhost -S mix