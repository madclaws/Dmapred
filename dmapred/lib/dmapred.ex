defmodule Dmapred do
  @moduledoc """
  Documentation for `Dmapred`.
  """
  @callback map_fn(key :: any(), value :: any()) :: list({key :: any(), value :: any()})
  @callback reduce_fn(key :: any(), values :: list()) :: any()
end
