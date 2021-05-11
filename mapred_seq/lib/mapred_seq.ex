defmodule MapredSeq do
  @moduledoc """
  A simple sequential MapReduce.
  """

  @callback map_fn(key :: any(), value :: any()) :: list({key :: any(), value :: any()})
  @callback reduce_fn(key :: any(), values :: list()) :: any()

  def run(module, dirname \\ "../resources") do
    intermediate_output =
      File.ls!(dirname)
      |> Enum.reduce([], fn filename, intermediate ->
        {:ok, content} = File.read("#{dirname}/#{filename}")

        ####  Intermediate output generation ##########
        intermediate ++ module.map_fn("#{dirname}/#{filename}", content)
      end)

    # a big difference from real MapReduce is that all the
    # intermediate data is in one place, intermediate[],
    # rather than being partitioned into NxM buckets.

    # Sorting the KV pairs according to key.
    intermediate_output = Enum.sort(intermediate_output)
    intermediate_chunks = Enum.chunk_by(intermediate_output, fn {k, _v} -> k end)

    output_filename = "mr-out-0"
    io_device = File.open!(output_filename, [:write, :append, :utf8])
    process_data_for_reduce(intermediate_chunks, module, io_device)
    File.close(io_device)
  end

  defp process_data_for_reduce([], _module, _device), do: []

  defp process_data_for_reduce([h | t], module, device) do
    [{key, _value} | _rest_kv] = h
    value_list = for {_k, v} <- h, do: v
    output = module.reduce_fn(key, value_list)
    IO.write(device, "#{key} #{output}\n")
    process_data_for_reduce(t, module, device)
  end
end
