defmodule Dmapred.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      get_process_spec(Application.fetch_env!(:dmapred, :type))
    ]

    opts = [strategy: :one_for_one, name: Dmapred.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @spec get_process_spec(type :: atom()) :: {atom(), list()}
  defp get_process_spec(:master), do: {Dmapred.Master, [name: :master]}

  defp get_process_spec(type), do: {Dmapred.Worker, [name: type]}
end
