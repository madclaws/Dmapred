defmodule Dmapred do
  @moduledoc """
  Entrypoint of Dmapred library

  This module contains the public API's to use Distributed Mapreduce
  """
  require Logger
  @callback map_fn(key :: any(), value :: any()) :: list({key :: any(), value :: any()})
  @callback reduce_fn(key :: any(), values :: list()) :: any()

end
