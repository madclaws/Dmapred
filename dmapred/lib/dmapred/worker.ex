defmodule Dmapred.Worker do
  @moduledoc """
  Executes map/reduce tasks given by master, write the respective outputs
  on disk and notify master.
  """

  use GenServer

  require Logger

  @ping_interval 5_000

  @type worker_state :: %{
          name: atom(),
          status: :idle | :busy
        }

  # Client functions

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:global, Keyword.fetch!(opts, :name)})
  end

  # server callbacks
  @impl true
  @spec init(keyword()) :: {:ok, worker_state()}
  def init(name: process_name) do
    Logger.info("Worker started #{inspect(process_name)}")
    connect_to_master()
    {:ok, %{name: process_name, status: :idle}}
  end

  @impl true
  def handle_cast({"request_task", worker_id}, state) do
    task = Dmapred.Master.give_task(worker_id)
    IO.puts("Task Received \n #{inspect(task)}")

    # A 1 second delay is to set the worker busy
    Process.send_after(self(), {:execute_task, task}, 1_000)
    {:noreply, %{state | status: :busy}}
  end

  @impl true
  def handle_call(:stop_worker, _from, state) do
    Logger.warn("Stopping worker #{inspect(state.name)}")
    :init.stop()
    {:reply, :ok, state}
  end

  @impl true
  def handle_info("ping_master", %{name: name, status: :idle} = worker_state) do
    Logger.warn("#{name} is idle, requesting task from master")
    GenServer.cast(self(), {"request_task", name})
    Process.send_after(self(), "ping_master", @ping_interval)
    {:noreply, worker_state}
  end

  def handle_info("ping_master", %{name: name} = worker_state) do
    Logger.warn("#{name} is busy")
    Process.send_after(self(), "ping_master", @ping_interval)
    {:noreply, worker_state}
  end

  def handle_info({:execute_task, task}, state) do
    case execute_task(task) do
      :ok -> Dmapred.Master.task_completed(state.name, task.id, task.type)
      _ -> true
    end

    {:noreply, %{state | status: :idle}}
  end

  @impl true
  def terminate(reason, %{name: name} = _state) do
    Logger.warn("Terminating worker-#{inspect(name)} due to #{inspect(reason)}")
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

  @spec execute_task(task :: Dmapred.Task.t()) :: :ok
  defp execute_task(%Dmapred.Task{type: :map} = task) do
    # (k, v) -> List({k, v})
    content = File.read!(task.input)
    intermediate = task.app.map_fn(task.input, content)
    store_intermediate_output(intermediate, task.id)
    Logger.warn("Map task #{task.id} finished...")
    :ok
  end

  defp execute_task(%Dmapred.Task{type: :reduce} = task) do
    # (k, List(v)) -> (k, v)
    reducer = task.id - 1

    intermediate_files =
      File.ls!("intermediates/")
      |> Enum.filter(&is_correct_reducer_file?(&1, reducer))

    Logger.info(inspect(intermediate_files))
    io_device = File.open!("outputs/mr-out-#{reducer}", [:write, :append, :utf8])

    concatenate_files(intermediate_files)
    |> apply_reduce_for_list(io_device, task.app)

    File.close(io_device)
    :ok
  end

  defp execute_task(_task) do
    Logger.info("Invalid Task")
    :error
  end

  defp is_correct_reducer_file?(file, reducer) do
    String.split(file, "-")
    |> List.last()
    |> String.to_integer() === reducer
  end

  @spec store_intermediate_output(kv_list :: list(), task_id :: number()) :: :ok
  defp store_intermediate_output(kv_list, task_id) do
    intermediate_chunks =
      Enum.sort(kv_list)
      |> Enum.chunk_by(fn {k, _v} -> k end)

    write_to_file(intermediate_chunks, task_id)
  end

  defp write_to_file(intermediate_chunks, task_id, io_devices \\ %{})

  defp write_to_file([], _task_id, io_devices) do
    Enum.each(io_devices, fn {_file, io_device} ->
      File.close(io_device)
    end)
  end

  defp write_to_file([h | t], task_id, io_devices) do
    [{key, _value} | _rest_kv] = h
    map_task_id = task_id
    reduce_task_id = rem(:erlang.phash2(key), Application.fetch_env!(:dmapred, :nreduce))
    filename = "intermediates/mr-#{map_task_id}-#{reduce_task_id}"

    io_device =
      case Map.has_key?(io_devices, filename) do
        true -> Map.get(io_devices, filename)
        _ -> File.open!(filename, [:write, :append, :utf8])
      end

    Enum.each(h, fn {k, v} ->
      IO.write(io_device, "#{k}:#{v}\n")
    end)

    write_to_file(
      t,
      task_id,
      Map.update(io_devices, filename, io_device, fn _device -> io_device end)
    )
  end

  defp concatenate_files(intermediate_files) do
    concatenated_list =
      Enum.reduce(intermediate_files, [], fn file, concat_list ->
        content_list =
          File.read!("intermediates/#{file}")
          |> String.trim()
          |> String.split("\n")

        concat_list ++ content_list
      end)

    Enum.sort(concatenated_list)
  end

  defp apply_reduce_for_list(concat_list, io_device, app) do
    reduce_by_chunk(concat_list, app)
    |> then(&IO.write(io_device, &1))
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
