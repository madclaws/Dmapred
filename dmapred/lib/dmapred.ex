defmodule Dmapred do
  @moduledoc """
  Entrypoint of Dmapred library

  This module contains the public API's to use Distributed Mapreduce
  """
  require Logger
  @callback map_fn(key :: any(), value :: any()) :: list({key :: any(), value :: any()})
  @callback reduce_fn(key :: any(), values :: list()) :: any()

  @doc """
  Public API to execute Distributed MapReduce task.

  - `input_location` - The directory where input files are located.
  - `app`            - The mapreduce app to run (ex WordCount).
  - `nReduce`        - Total no:of Reduce tasks to spawn

  Returns `:ok`, when the task is done.
  """
  @spec map_reduce(input_location :: String.t(), app :: atom(), nReduce :: number()) :: atom()
  def map_reduce(input_location, app, nReduce \\ 10)

  def map_reduce(input_location, app, nReduce) do
    Logger.info("input location => #{inspect(input_location)}")
    Logger.info("App => #{inspect(app)}")
    Logger.info("Reducer count => #{inspect(nReduce)}")
  end
end
