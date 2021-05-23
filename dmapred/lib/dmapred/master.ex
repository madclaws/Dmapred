defmodule Dmapred.Master do
  @moduledoc """
  Orchestrates all the map/reduce tasks distributing across, mulitple workers
  and handles fault tolerance.
  """

  use GenServer
  require Logger
  # Client functions

  @type master_state :: %{
          name: atom(),
          input_files: list(String.t()),
          tasks: %{},
          task_count: number()
        }

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:global, :master})
  end

  def give_task(worker_id) do
    GenServer.call({:global, :master}, {"on_give_task", worker_id})
  end

  # server callbacks
  @impl true
  @spec init(any) :: {:ok, master_state()}
  def init(opts) do
    Logger.info("Master started #{inspect(opts)}")
    dirname = "../resources"
    input_files = File.ls!(dirname) |> Enum.map(fn input_file -> "#{dirname}/#{input_file}" end)
    {:ok, %{name: :master, input_files: input_files, tasks: %{}, task_count: 0}}
  end

  @impl true
  def handle_call({"on_give_task", worker_id}, _from, state) do
    {task, state} = get_task(worker_id, state)
    {:reply, task, state}
  end

  # helper functions

  @spec get_task(atom(), master_state()) :: {Dmapred.Task.t() | nil, master_state()}
  defp get_task(_worker_id, %{input_files: []} = state), do: get_idle_task(state)

  defp get_task(worker_id, %{input_files: [h | t], task_count: tcount} = state) do
    task = create_map_task(h, tcount + 1, worker_id)

    state = %{
      state
      | tasks: Map.update(state.tasks, task.id, task, fn _ -> task end),
        task_count: state.task_count + 1,
        input_files: t
    }

    {task, state}
  end

  @spec create_map_task(String.t(), number(), atom()) :: Dmapred.Task.t()
  defp create_map_task(input_file, id, worker_id) do
    Dmapred.Task.new(id, :map, :in_progress, worker_id, input_file)
  end

  @spec get_idle_task(master_state()) :: {Dmapred.Task.t() | nil, master_state()}
  defp get_idle_task(%{tasks: tasks} = state) do
    idle_tasks = Enum.filter(tasks, fn {_task_id, task} ->
      task.status === :idle
    end)
    case List.first(idle_tasks) do
      nil -> {nil, state}
      idle_task -> task = Map.values(idle_task) |> List.first() |> Map.update!(:status, fn _status -> :in_progress end)
                   {task, %{state | tasks: Map.update(state.tasks, task.id, task, fn _ -> task end)}}
    end
  end
end
