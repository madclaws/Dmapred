defmodule Dmapred.Master do
  @moduledoc """
  Orchestrates all the map/reduce tasks distributing across, mulitple workers
  and handles fault tolerance.
  """

  use GenServer
  require Logger

  @worker_timeout 10_000
  # Client functions

  @type master_state :: %{
          name: atom(),
          input_files: list(String.t()),
          # Map of Dmapred.Task.t()
          tasks: %{},
          task_count: number(),
          app: atom(),
          nMap: number(),
          nReduce: number(),
          map_tasks_finished: number(),
          reduce_tasks_finished: number()
        }

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:global, :master})
  end

  def give_task(worker_id) do
    GenServer.call({:global, :master}, {"on_give_task", worker_id})
  end

  @doc """
  Handles when a task is completed by a worker
  - worker_id - worker name atom
  - task_id - id of the task
  """
  def task_completed(worker_id, task_id, task_type) do
    GenServer.cast({:global, :master}, {:on_task_completed, worker_id, task_id, task_type})
  end

  # server callbacks
  @impl true
  @spec init(any) :: {:ok, master_state()}
  def init(opts) do
    Logger.info("Master inited #{inspect(opts)}")
    input_location = Application.fetch_env!(:dmapred, :files)

    input_files =
      File.ls!(input_location) |> Enum.map(fn input_file -> "#{input_location}/#{input_file}" end)

    state =
      %{
        name: :master,
        input_files: input_files,
        tasks: %{},
        task_count: 0,
        app: Application.fetch_env!(:dmapred, :app),
        nReduce: Application.fetch_env!(:dmapred, :nreduce),
        nMap: Enum.count(input_files),
        map_tasks_finished: 0,
        reduce_tasks_finished: 0
      }
      |> then(&create_reduce_tasks(0, &1))
      |> tap(&IO.inspect(&1))

    Process.send_after(self(), :check_mapreduce_completion, 3_000)
    {:ok, state}
  end

  @impl true
  def handle_call({"on_give_task", worker_id}, _from, state) do
    {task, state} = get_task(worker_id, state)
    IO.puts("Task generated => #{inspect(task)}")
    start_task_timer(task)
    {:reply, task, state}
  end

  @impl true
  def handle_info({:worker_timeout, task_type, task_id}, %{tasks: tasks} = state) do
    IO.puts("Timeout for #{inspect(task_type)}-#{inspect(task_id)}....")
    #     if the status is still :in_progress -> set the status back to idle
    #     Have to deal later with temp files.
    task =
      case Map.fetch!(tasks[task_id], :status) do
        :completed ->
          IO.puts("Task #{inspect(task_type)}-#{inspect(task_id)} already completed..")
          tasks[task_id]

        :in_progress ->
          IO.puts("Task #{inspect(task_type)}-#{inspect(task_id)} still in progress..")
          Map.update!(tasks[task_id], :status, fn _ -> :idle end)

        _ ->
          Logger.warn("Invalid state reached, nil task should not be there")
      end

    {:noreply, %{state | tasks: Map.update!(state.tasks, task_id, fn _ -> task end)}}
  end

  @impl true
  def handle_info(
        :check_mapreduce_completion,
        %{nReduce: count, reduce_tasks_finished: count} = state
      ) do
    Logger.warn("MapReduce operation finished!!")
    stop_workers()
    :init.stop()
    {:noreply, state}
  end

  def handle_info(:check_mapreduce_completion, state) do
    Process.send_after(self(), :check_mapreduce_completion, 3_000)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:on_task_completed, worker_id, task_id, task_type}, %{tasks: tasks} = state) do
    IO.puts("Task #{inspect(task_type)} #{task_id} completed by #{inspect(worker_id)}")

    {completed_task, can_increment} =
      case is_valid_event?(tasks[task_id], worker_id) do
        true ->
          IO.puts("Valid completed event")
          {Map.update!(tasks[task_id], :status, fn _ -> :completed end), true}

        _ ->
          IO.puts("Invalid completed event")
          {tasks[task_id], false}
      end

    # TODO: Managing temp files
    {:noreply,
     %{
       state
       | tasks: Map.update!(state.tasks, task_id, fn _ -> completed_task end),
         map_tasks_finished:
           increment_finished_count(can_increment, state.map_tasks_finished, task_type, :map),
         reduce_tasks_finished:
           increment_finished_count(
             can_increment,
             state.reduce_tasks_finished,
             task_type,
             :reduce
           )
     }}
  end

  ###################
  # helper functions
  ###################

  @spec increment_finished_count(
          can_increment :: boolean(),
          finished_count :: number(),
          task_type :: :map | :reduce,
          atom()
        ) :: number()

  defp increment_finished_count(false, finished_count, _, _), do: finished_count
  defp increment_finished_count(true, finished_count, :map, :map), do: finished_count + 1
  defp increment_finished_count(true, finished_count, :reduce, :reduce), do: finished_count + 1
  defp increment_finished_count(true, finished_count, _, _), do: finished_count

  @spec get_task(atom(), master_state()) :: {Dmapred.Task.t() | nil, master_state()}
  defp get_task(worker_id, %{input_files: []} = state), do: get_idle_task(worker_id, state)

  defp get_task(worker_id, %{input_files: [h | t], task_count: tcount} = state) do
    task = create_map_task(h, tcount + 1, worker_id, state.app)

    state = %{
      state
      | tasks: Map.update(state.tasks, task.id, task, fn _ -> task end),
        task_count: state.task_count + 1,
        input_files: t
    }

    {task, state}
  end

  @spec create_map_task(String.t(), number(), atom(), atom()) :: Dmapred.Task.t()
  defp create_map_task(input_file, id, worker_id, app) do
    Dmapred.Task.new(id, :map, :in_progress, worker_id, input_file, app)
  end

  @spec get_idle_task(worker_id :: atom(), master_state()) ::
          {Dmapred.Task.t() | nil, master_state()}
  defp get_idle_task(worker_id, %{tasks: tasks} = state) do
    case get_priority_task(tasks, state) do
      nil ->
        {nil, state}

      idle_task ->
        idle_task
        |> tap(&IO.puts(inspect(&1)))
        |> Map.update!(:status, fn _status -> :in_progress end)
        |> Map.update!(:worker, fn _ -> worker_id end)
        |> then(fn idle_task ->
          {idle_task,
           %{
             state
             | tasks: Map.update(state.tasks, idle_task.id, idle_task, fn _ -> idle_task end)
           }}
        end)
    end
  end

  @spec get_priority_task(tasks :: map(), state :: master_state()) :: Dmapred.Task.t()
  defp get_priority_task(tasks, state) do
    tasks
    |> get_idle_map_task(state)
  end

  defp get_idle_map_task(tasks, state) do
    # Damn, this comprehension go throughs a list of maps of type {id, Dmapred.Task.t}, and then
    # ignores all tasks where status is not idle and then returns only task to the final list

    idle_tasks = for {_id, %{status: :idle} = task} <- tasks, do: task

    Enum.filter(idle_tasks, fn task -> task.type === :map end)
    |> get_idle_reduce_task(idle_tasks, state)
  end

  @spec get_idle_reduce_task(
          idle_map_tasks :: list(Dmapred.Task.t()),
          idle_tasks :: list(Dmapred.Task.t()),
          state :: master_state()
        ) :: Dmapred.Task.t()

  defp get_idle_reduce_task([], idle_tasks, state) do
    case are_map_tasks_finished?(state) do
      true ->
        Enum.filter(idle_tasks, fn task -> task.type === :reduce end)
        |> List.first()

      _ ->
        nil
    end
  end

  defp get_idle_reduce_task(idle_map_tasks, _idle_tasks, _state), do: List.first(idle_map_tasks)

  defp are_map_tasks_finished?(state) do
    # Add a new field map_tasks_finished and update on map complettion.
    # just compare
    state.nMap === state.map_tasks_finished
  end

  @spec start_task_timer(task :: Dmapred.Task.t()) :: any()
  defp start_task_timer(nil), do: :ok

  defp start_task_timer(%Dmapred.Task{} = task) do
    Process.send_after(self(), {:worker_timeout, task.type, task.id}, @worker_timeout)
  end

  @spec is_valid_event?(task :: Dmapred.Task.t(), worker :: atom()) :: boolean()
  defp is_valid_event?(%Dmapred.Task{worker: worker, status: :in_progress} = _task, worker) do
    true
  end

  defp is_valid_event?(_task, _worker), do: false

  @spec create_reduce_tasks(task_count :: number(), state :: master_state(), tasks: map()) ::
          master_state()
  defp create_reduce_tasks(task_count, state, tasks \\ %{})

  defp create_reduce_tasks(task_count, %{nReduce: task_count} = state, tasks) do
    Map.update!(state, :tasks, fn _ -> tasks end)
    |> Map.update!(:task_count, fn _ -> task_count end)
  end

  defp create_reduce_tasks(task_count, state, tasks) do
    reduce_task =
      Dmapred.Task.__struct__(
        id: task_count + 1,
        type: :reduce,
        app: state.app
      )

    create_reduce_tasks(
      task_count + 1,
      state,
      Map.update(tasks, reduce_task.id, reduce_task, fn reduce_task -> reduce_task end)
    )
  end

  def stop_workers do
    Node.list()
    |> Enum.each(fn node ->
      worker_name = Atom.to_string(node) |> String.split("@") |> List.first() |> String.to_atom()
      GenServer.call({:global, worker_name}, :stop_worker)
    end)
  end
end
