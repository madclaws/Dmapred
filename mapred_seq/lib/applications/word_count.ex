defmodule WordCount do
  @moduledoc """
  Implements wordcount program using Mapreduce programming model.
  """
  @behaviour MapredSeq

  @impl true
  def map_fn(key, value) do
    IO.puts("counting #{key}")

    value
    # This was cooool
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

  # Regex using and was used
  # \W -> \W -> Matches any character , ie not a word character(alphanumeric + underscore)
  # [^a-zA-Z] -> matches all non alphabets.

  # Fuck evolution, it affects some :)
  # Fuck songs, it affects some :)

  # 39, will miss you
end
