defmodule WordCount do
  @moduledoc """
  Implements wordcount program using Mapreduce programming model.
  """
  @behaviour MapredSeq

  @impl true
  def map_fn(key, value) do
    IO.puts("counting #{key}")

    value
    |> String.split(~r{[^a-zA-Z]})
    |> Enum.filter(fn str -> str !== "" end)
    |> Enum.reduce([], fn word, kv_list ->
      [{String.upcase(word), "1"} | kv_list]
    end)
  end

  @impl true
  def reduce_fn(_key, values) do
    Enum.count(values)
  end

end
