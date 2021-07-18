defmodule DmapredTest.MasterTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  @tag :skip
  test "worker_timeout handle_info/2" do
    pid = start_supervised!({Dmapred.Master, [name: :master]})
    worker_pid = start_supervised!({Dmapred.Worker, [name: :worker1]})

    GenServer.cast(worker_pid, {"request_task", :worker1})
    tmp_state = :sys.get_state(pid)
    IO.puts(inspect(tmp_state[:tasks][1]))
    Process.sleep(12_000)
    tmp_state = :sys.get_state(pid)
    IO.puts(inspect(tmp_state[:tasks][1]))
    Process.sleep(10_000)
  end

  @tag :skip
  test "get_idle_task/2" do
    pid = start_supervised!({Dmapred.Master, [name: :master]})
    worker_pid = start_supervised!({Dmapred.Worker, [name: :worker1]})

    GenServer.cast(worker_pid, {"request_task", :worker1})
    # tmp_state = :sys.get_state(pid)
    # IO.puts(inspect(tmp_state[:tasks][1]))
    Process.sleep(10_000)
    # tmp_state = :sys.get_state(pid)
    # IO.puts(inspect(tmp_state[:tasks][1]))
    # Process.sleep(10_000)
  end
end
