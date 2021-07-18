defmodule Dmapred.Worker do
  @moduledoc """
  Executes map/reduce tasks given by master, write the respective outputs
  on disk and notify master.
  """

  use GenServer

  require Logger

  @type worker_state :: %{
          name: atom(),
          status: :idle | :busy
        }

  # Client functions

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:global, Keyword.fetch!(opts, :name)})
  end

  def reduce_task(worker_id) do
    GenServer.cast({:global, worker_id}, "reduce_task_test")
  end

  # server callbacks
  @impl true
  @spec init(any) :: {:ok, worker_state()}
  def init(name: process_name) do
    Logger.info("Worker started #{inspect(process_name)}")
    connect_to_master()
    {:ok, %{name: process_name, status: :idle}}
  end

  @impl true
  def handle_cast({"request_task", worker_id}, state) do
    task = Dmapred.Master.give_task(worker_id)
    Logger.info("Task Received #{inspect(task)}")
    execute_task(task)
    {:noreply, state}
  end

  @impl true
  def handle_cast("reduce_task_test", state) do
    execute_task(%Dmapred.Task{
      id: 2,
      type: :reduce,
      status: :in_progress,
      input: "",
      worker: :worker_2,
      app: WordCount
    })

    {:noreply, state}
  end

  @impl true
  def handle_info("ping_master", %{name: name, status: :idle} = worker_state) do
    GenServer.cast(self(), {"request_task", name})
    Process.send_after(self(), "ping_master", 3_000)
    {:noreply, worker_state}
  end

  def handle_info("ping_master", worker_state) do
    Process.send_after(self(), "ping_master", 3_000)
    {:noreply, worker_state}
  end

  @spec connect_to_master() :: any()
  defp connect_to_master do
    case Node.connect(:master@localhost) do
      true ->
        Logger.info("Connected to master")
        Process.send_after(self(), "ping_master", 3_000)

      _ ->
        Logger.info("Failed: Connecting to master")
    end
  end

  defp execute_task(%Dmapred.Task{type: :map} = task) do
    # (k, v) -> List({k, v})
    content = File.read!(task.input)
    intermediate = task.app.map_fn(task.input, content)
    store_intermediate_output(intermediate)
  end

  defp execute_task(%Dmapred.Task{type: :reduce} = task) do
    # (k, List(v)) -> (k, v)
    reducer = 2

    intermediate_files =
      File.ls!("intermediates/")
      |> Enum.filter(&is_correct_reducer_file?(&1, reducer))

    Logger.info(inspect(intermediate_files))
    io_device = File.open!("outputs/mr-out-#{reducer}", [:write, :append, :utf8])
    Enum.each(intermediate_files, &apply_reduce_for_file(&1, io_device, task.app))
    File.close(io_device)
  end

  defp execute_task(_task), do: Logger.info("Invalid Task")

  defp is_correct_reducer_file?(file, reducer) do
    String.split(file, "-")
    |> List.last()
    |> String.to_integer() === reducer
  end

  defp store_intermediate_output(kv_list) do
    intermediate_chunks =
      Enum.sort(kv_list)
      |> Enum.chunk_by(fn {k, _v} -> k end)

    write_to_file(intermediate_chunks)
  end

  defp write_to_file(intermediate_chunks, io_devices \\ %{})

  defp write_to_file([], io_devices) do
    Enum.each(io_devices, fn {_file, io_device} ->
      File.close(io_device)
    end)
  end

  defp write_to_file([h | t], io_devices) do
    [{key, _value} | _rest_kv] = h
    x = 1
    y = rem(:erlang.phash2(key), 10)
    filename = "intermediates/mr-#{x}-#{y}"

    io_device =
      case Map.has_key?(io_devices, filename) do
        true -> Map.get(io_devices, filename)
        _ -> File.open!(filename, [:write, :append, :utf8])
      end

    Enum.each(h, fn {k, v} ->
      IO.write(io_device, "#{k}:#{v}\n")
    end)

    write_to_file(t, Map.update(io_devices, filename, io_device, fn _device -> io_device end))
  end

  defp apply_reduce_for_file(intermediate_file, io_device, app) do
    reduce_output =
      File.read!("intermediates/#{intermediate_file}")
      |> String.trim()
      |> String.split("\n")
      |> reduce_by_chunk(app)

    IO.write(io_device, reduce_output)
  end

  defp reduce_by_chunk(lines, app, last_key \\ "", value_list \\ [], output \\ "")

  defp reduce_by_chunk([], app, last_key, value_list, output) do
    reduce_value = app.reduce_fn(last_key, value_list)
    "#{last_key}:#{reduce_value}\n#{output}"
  end

  defp reduce_by_chunk([h | t], app, last_key, value_list, output) do
    [key, value] = String.split(h, ":")

    {n_output, n_value_list} =
      cond do
        last_key === "" ->
          {output, [value | value_list]}

        last_key === key ->
          {output, [value | value_list]}

        true ->
          reduce_value = app.reduce_fn(last_key, value_list)
          {"#{last_key}:#{reduce_value}\n#{output}", [value]}
      end

    reduce_by_chunk(t, app, key, n_value_list, n_output)
  end
end
