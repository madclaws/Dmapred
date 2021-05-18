defmodule Dmapred.Worker do
  @moduledoc """
  Executes map/reduce tasks given by master, write the respective outputs
  on disk and notify master.
  """

  use GenServer

  require Logger
  # Client functions

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: {:global, {__MODULE__, name: Keyword.fetch!(opts, :name)}}
    )
  end

  # server callbacks
  @impl true
  @spec init(any) :: {:ok, map()}
  def init(name: process_name) do
    Logger.info("Worker started #{inspect(process_name)}")
    connect_to_master()
    {:ok, %{name: process_name}}
  end

  @spec connect_to_master() :: any()
  defp connect_to_master do
    case Node.connect(:master@localhost) do
      true -> Logger.info("Connected to master")
      _ -> Logger.info("Failed: Connecting to master")
    end
  end
end
