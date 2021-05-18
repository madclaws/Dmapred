defmodule Dmapred.Master do
  @moduledoc """
  Orchestrates all the map/reduce tasks distributing across, mulitple workers
  and handles fault tolerance.
  """

  use GenServer
  require Logger
  # Client functions

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: {:global, :master}
    )
  end

  @spec run(dirname :: String.t()) :: any()
  def run(dirname \\ "../resources")

  def run(dirname) do
    GenServer.call({:global, :master}, {"run", dirname})
  end

  # server callbacks
  @impl true
  @spec init(any) :: {:ok, map()}
  def init(opts) do
    Logger.info("Master started #{inspect opts}")
    {:ok, %{name: :master, input_files: [], tasks: %{}}}
  end

  @impl true
  def handle_call({"run", dirname}, _from, state) do
    input_files = File.ls!(dirname) |> Enum.each(fn input_file -> "#{dirname}/#{input_file}" end)
    {:reply, state, %{state | input_files: input_files}}
  end

  # helper functions

  @spec get_task(state :: map()) :: Dmapred.Task.t()
  defp get_task(state) do

  end


  defp get_new_map_task do

  end

  defp get_idle_task do

  end
end
